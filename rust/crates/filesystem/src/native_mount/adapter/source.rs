impl<A, O> CheckoutMountSource<A, O> {
    pub(super) fn callback_guard(&self) -> Option<&Arc<dyn super::NativeMountCallbackGuard>> {
        self.native_callback_guard.as_ref()
    }

    pub(super) fn native_original_work(
        &self,
    ) -> Result<Option<crate::NativeMaterializationWork>, MountSourceError> {
        self.cancellation
            .check()
            .map_err(|_| MountSourceError::Stale)?;
        Ok(self.native_original.clone())
    }

    pub(super) fn captures_pending(&self) -> bool {
        self.capture_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .producers
            != 0
    }

    /// Whether the volume resolves every case of a name to one entry.
    pub(super) fn folds_names(&self) -> bool {
        self.folds_names
    }

    pub(super) fn path_is_unchanged(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<bool, MountSourceError> {
        let cancellation = carrier.cancellation();
        carrier.check(&cancellation, Some(&self.cancellation))?;
        let mut inputs = InputBuffers::new(carrier)?;
        let path = self.namespace_inputs(path, &mut inputs)?;
        inputs.items(1)?;
        let unchanged = self.checkout.view_gate.is_stable()
            && inputs.carrier.with_work(|work| {
                self.checkout.ledger.unchanged_since(&path, file_id, stamp, work)
            })?;
        inputs.carrier.check(&cancellation, Some(&self.cancellation))?;
        Ok(unchanged)
    }

    pub(super) fn shared_checkout(&self) -> &SharedCheckout<A, O> {
        &self.checkout
    }

    pub(super) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// The checkout's current generation, which publication advances.
    #[cfg(all(test, target_os = "macos"))]
    pub(super) fn generation_id(&self) -> Result<crate::GenerationId, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            Ok(self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?
                .generation_id())
        })
    }

    /// The checkout path a mounted path names, as view changes key it.
    pub(super) fn namespace_path(
        &self,
        path: &MountPath,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(NamespacePath, Option<crate::NativeAllocation>), MountSourceError> {
        let mut inputs = InputBuffers::new(carrier)?;
        let path = self.namespace_inputs(path, &mut inputs)?;
        Ok((path, inputs.retain()))
    }

    /// Records a change a composing source makes to its own projection of
    /// this checkout. The caller holds that source's exclusive view, so no
    /// lookup through it overlaps the change.
    pub(super) fn record_projection_change(
        &self,
        change: &ViewChange<'_>,
        carrier: &mut NativeDemandCarrier<'_>,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        mut private_check: impl FnMut() -> Result<(), MountSourceError>,
    ) -> Result<(), MountSourceError> {
        private_check()?;
        let recorded = self.checkout.ledger.record(change, carrier, view);
        let checked = private_check();
        recorded.and(checked)
    }

    /// Binds an existing source handle to the same stable identity once a
    /// peer stages that identity in the mounted checkout candidate.
    pub(super) fn attached_file_by_id(
        &self,
        file_id: FileId,
    ) -> Result<Option<Arc<dyn MountOpenFile>>, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let owner = ViewGate::callback_owner();
        let present = self.runtime.wait(|| async {
            let observation = self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?;
            let mut checkout = observation
                .observer(boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value;
            match checkout
                .read_file_record_by_id(file_id, boundary_budget(), &self.cancellation)
                .await
            {
                Ok(record) if record.value.kind == FileKind::Regular => Ok(true),
                Ok(_) => Err(MountSourceError::Stale),
                Err(failure) if matches!(failure.error, FsError::NotFound) => Ok(false),
                Err(failure) => Err(engine_error(failure)),
            }
        })?;
        Ok(present.then(|| {
            Arc::new(CheckoutAttachedFile {
                checkout: Arc::clone(&self.checkout),
                file_id,
                runtime: self.runtime.clone(),
                cancellation: self.cancellation.clone(),
                profile: self.profile,
                limits: self.limits,
            }) as Arc<dyn MountOpenFile>
        }))
    }

    pub(super) async fn lookup_in(
        &self,
        checkout: &Checkout<A, O>,
        path: &MountPath,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = carrier.cancellation();
        carrier.check(&cancellation, Some(&self.cancellation))?;
        let mut inputs = InputBuffers::new(carrier)?;
        let path = self.namespace_inputs(path, &mut inputs)?;
        let mut inspector = if let Some(work) = inputs.carrier.original_native_work() {
            checkout.inspector(&work, &cancellation).map_err(fs_error)?
        } else {
            let (budget, live) = inputs.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(super::lazy::mount_demand_error)?,
                    work.live_allocation_bytes(),
                ))
            })?;
            inputs.carrier.accept_with_prior_live(
                checkout.inspector(budget, &cancellation),
                live,
                fs_error,
            )?
        };
        inputs.adopt_checkout(&inspector)?;
        self.read_lookup(&mut inspector, &path, &mut inputs, &cancellation)
            .await
    }

    pub(super) async fn lookup_async(
        &self,
        path: &MountPath,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = carrier.cancellation();
        carrier.check(&cancellation, Some(&self.cancellation))?;
        let mut inputs = InputBuffers::new(carrier)?;
        let path = self.namespace_inputs(path, &mut inputs)?;
        let observation = self
            .checkout
            .observe(
                owner,
                inputs.carrier,
                &cancellation,
                Some(&self.cancellation),
            )
            .await?;
        let mut checkout = if let Some(work) = inputs.carrier.original_native_work() {
            observation
                .observer(&work, &cancellation)
                .map_err(fs_error)?
        } else {
            let (budget, live) = inputs.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(super::lazy::mount_demand_error)?,
                    work.live_allocation_bytes(),
                ))
            })?;
            inputs.carrier.accept_with_prior_live(
                observation.observer(budget, &cancellation),
                live,
                fs_error,
            )?
        };
        inputs.adopt_checkout(&checkout)?;
        self.read_lookup(&mut checkout, &path, &mut inputs, &cancellation)
            .await
    }

    async fn read_lookup(
        &self,
        checkout: &mut Checkout<A, O>,
        path: &NamespacePath,
        inputs: &mut InputBuffers<'_, '_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let result = if let Some(work) = inputs.carrier.original_native_work() {
            checkout
                .lookup_no_follow_with_metadata(path, &work, cancellation)
                .await
                .map_err(fs_error)
        } else {
            let (budget, live) = inputs.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(super::lazy::mount_demand_error)?,
                    work.live_allocation_bytes(),
                ))
            })?;
            let result = checkout
                .lookup_no_follow_with_metadata(path, budget, cancellation)
                .await;
            checkout.proof_data_take_completed_work();
            inputs
                .carrier
                .accept_with_prior_live(result, live, fs_error)
        };
        let value = result?;
        inputs
            .carrier
            .check(cancellation, Some(&self.cancellation))?;
        if value.is_some() {
            inputs.charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<MountLookup>() as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
        }
        Ok(value.map(|value| MountLookup {
            node: mount_node(value.record),
            metadata: value.metadata,
        }))
    }

    pub(super) fn record_by_id(&self, file_id: FileId) -> Result<FileRecord, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?;
            observation.ensure_publication_resolved()?;
            observation
                .observer(boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value
                .read_file_record_by_id(file_id, boundary_budget(), &self.cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    pub(super) fn detached_file_from_record(
        &self,
        record: FileRecord,
        metadata: FileMetadata,
    ) -> Result<Arc<CheckoutDetachedFile<A, O>>, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        let owner = ViewGate::callback_owner();
        let file = self.runtime.wait(|| async {
            Ok(self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?
                .detached_from_record(record, boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value)
        })?;
        Ok(Arc::new(CheckoutDetachedFile {
            state: tokio::sync::Mutex::new(DetachedMountState {
                file,
                metadata,
                mutation_epoch: 0,
            }),
            ledger: Arc::clone(&self.checkout.ledger),
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
        }))
    }

    /// Creates an independently cancellable adapter using the shared callback runtime.
    ///
    /// # Errors
    ///
    /// Returns a driver failure when the bounded callback runtime cannot start.
    pub fn new(
        checkout: Arc<SharedCheckout<A, O>>,
        config: VolumeConfig,
    ) -> Result<Self, NativeMountError> {
        let root = NamespacePath::new(Vec::new(), config.limits)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        Self::new_at(checkout, config, root)
    }

    /// Creates an adapter rooted at an authenticated workspace subtree.
    ///
    /// # Errors
    ///
    /// Returns a profile or runtime failure before any native mount starts.
    pub fn new_at(
        checkout: Arc<SharedCheckout<A, O>>,
        config: VolumeConfig,
        root: NamespacePath,
    ) -> Result<Self, NativeMountError> {
        if !profile_is_native(config.profile) {
            return Err(NativeMountError::ProfileUnavailable {
                profile: profile_name(config.profile),
                platform: std::env::consts::OS,
            });
        }
        let runtime = CallbackRuntime::create()?;
        Ok(Self {
            checkout,
            root,
            limits: config.limits,
            profile: config.profile,
            // Only UTF-8 names have case variants; the others fold to
            // themselves.
            folds_names: config.case_sensitivity == CaseSensitivity::ProfileFolded
                && matches!(
                    config.profile,
                    FilesystemProfile::Portable | FilesystemProfile::Browser
                ),
            cancellation: CancellationToken::new(),
            capture_gate: Arc::new(StdMutex::new(CaptureSourceGate::default())),
            capture_quiesced: Arc::new(std::sync::Condvar::new()),
            runtime,
            native_original: None,
            native_io_original: None,
            native_callback_guard: None,
            cleanup_credit: StdMutex::new(None),
            cleanup_final_credit: StdMutex::new(None),
            _namespace_allocation: None,
            _source_allocation: None,
        })
    }

    /// Whether `path` still names what it named after `stamp`, whatever
    /// changed beneath it.
    pub(super) fn binding_unchanged_since(&self, path: &MountPath, stamp: ViewStamp) -> bool {
        self.path(path)
            .is_ok_and(|path| self.checkout.binding_unchanged_since(&path, stamp))
    }

    /// Returns the checkout's owning volume without filesystem I/O.
    ///
    /// # Errors
    ///
    /// Returns stale when callback admission exceeds the finite deadline.
    pub fn volume_id(&self) -> Result<VolumeId, MountSourceError>
    where
        A: Send + Sync,
        O: Send + Sync,
    {
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            Ok(self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?
                .volume_id())
        })
    }

    /// Cancels future and in-flight canonical operations owned by this mount.
    pub fn cancel(&self) {
        self.capture_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancelled = true;
        self.cancellation.cancel();
    }

    /// Publishes every pending mount mutation regardless of automatic policy.
    ///
    /// # Errors
    ///
    /// Returns the exact fenced publication failure and retains retry identity.
    pub fn sync(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.seal(&self.cancellation).await
        })
    }

    /// Asynchronously publishes every pending mount mutation.
    ///
    /// This is the native customer handle path and does not enter the blocking
    /// foreign-thread callback bridge.
    ///
    /// # Errors
    ///
    /// Returns the exact fenced publication failure and retains retry identity.
    pub async fn sync_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut checkout = self.checkout.lock().await;
        checkout.seal(&self.cancellation).await
    }

    /// Publishes pending mutations only while the supplied operation lease is
    /// still active in the shared authority provider.
    pub async fn sync_async_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.sync_async_with_permit_budgeted(permit, boundary_budget())
            .await
    }

    pub(super) async fn sync_async_with_permit_budgeted(
        &self,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.mount.sync",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
        );
        let result = crate::obs::in_span(&span, async {
            let mut checkout = self.checkout.lock().await;
            checkout
                .seal_with_permit(permit, false, budget, &self.cancellation)
                .await
        })
        .await;
        crate::obs::outcome_on(&span, result)
    }

    pub(super) async fn sync_async_with_permit_force(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut checkout = self.checkout.lock().await;
        checkout
            .seal_with_permit(permit, true, boundary_budget(), &self.cancellation)
            .await
    }

    /// Safely advances a clean mounted checkout to the workspace head.
    ///
    /// Callers must publish pending native mutations first. Exact read
    /// dependencies are retained and checked against the candidate head;
    /// conflict leaves the mounted generation unchanged.
    pub async fn refresh_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.mount.refresh",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
        );
        let result = crate::obs::in_span(&span, async move {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let base = checkout
                .private_candidate(boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value;
            let decision = checkout
                .rebase_head(
                    self.limits.maximum_checkout_dependencies,
                    WorkBudget::UNBOUNDED,
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            match decision.value {
                RebaseDecision::Safe { .. } => {
                    self.record_advance(&mut checkout, &base).await?;
                    Ok(())
                }
                RebaseDecision::Conflicted { .. } => Err(MountSourceError::Stale),
            }
        })
        .await;
        crate::obs::outcome_on(&span, result)
    }

    /// Adopts the workspace head after this checkout's mutations were
    /// synchronized and a separate fenced operation published that head.
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.mount.advance_to_head",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
        );
        let result = crate::obs::in_span(&span, async move {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let base = checkout
                .private_candidate(boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value;
            checkout
                .refresh_head(WorkBudget::UNBOUNDED, &self.cancellation)
                .await
                .map_err(engine_error)?;
            self.record_advance(&mut checkout, &base).await?;
            Ok(())
        })
        .await;
        crate::obs::outcome_on(&span, result)
    }

    /// Records exactly what advancing the checkout from `base` changed in
    /// the view, as a binding transition; an advance that changed nothing,
    /// such as one to an unmoved head, records nothing, so drivers keep
    /// every cache and a revalidation barrier has nothing to wait for.
    async fn record_advance(
        &self,
        checkout: &mut SharedCheckoutGuard<'_, A, O>,
        base: &Checkout<A, O>,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        if checkout.root().file_table == base.root().file_table {
            return Ok(());
        }
        // Names that changed in directories at unknown paths are covered
        // by the whole checkout, whichever root a composing source mounts.
        let root =
            NamespacePath::new(Vec::new(), self.limits).map_err(MountSourceError::NamespacePath)?;
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        let (installed, _allocation) = installed_changes(
            base,
            checkout,
            InstallScope::Subtree(&root),
            &mut NativeDemandCarrier::Borrowed(&mut work),
            &self.cancellation,
        )
        .await?;
        self.checkout.view_gate.begin_transition();
        checkout.record(&ViewChange::Effect(&installed));
        self.checkout.view_gate.finish_transition();
        Ok(())
    }
}
