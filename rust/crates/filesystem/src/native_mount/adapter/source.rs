impl<A, O> CheckoutMountSource<A, O> {
    pub(super) fn callback_guard(&self) -> Option<&Arc<dyn super::NativeMountCallbackGuard>> {
        self.native_callback_guard.as_ref()
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
                self.checkout
                    .ledger
                    .unchanged_since(&path, file_id, stamp, work)
            })?;
        inputs
            .carrier
            .check(&cancellation, Some(&self.cancellation))?;
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
    pub(super) fn record_projection_change(
        &self,
        change: &ViewChange<'_>,
        carrier: &mut NativeDemandCarrier<'_>,
        mut private_check: impl FnMut() -> Result<(), MountSourceError>,
    ) -> Result<(), MountSourceError> {
        private_check()?;
        let recorded = self.checkout.ledger.record(change, carrier);
        let checked = private_check();
        recorded.and(checked)
    }

    /// Opens a regular file by its stable identity.
    pub(super) async fn attached_file_by_id(
        &self,
        file_id: FileId,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Option<(Arc<dyn MountOpenFile>, u64)>, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let record = match self.record_by_id(file_id, owner, carrier).await {
            Ok(record) => record,
            Err(MountSourceError::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        if record.kind != FileKind::Regular {
            return Err(MountSourceError::Stale);
        }
        self.attached_file(record.file_id, carrier).map(Some)
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

    async fn change_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Option<(&[u8], NamedAttributeWriteMode)>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = work.cancellation();
        let mut carrier = NativeDemandCarrier::Native(work);
        let mut inputs = InputBuffers::new(&mut carrier)?;
        let name = inputs.attribute_name(name, self.profile, self.limits)?;
        let (mut candidate, epoch, stamp) = self
            .checkout
            .candidate(None, inputs.carrier, cancellation, Some(&self.cancellation))
            .await?;
        let lookup = self
            .lookup_in(&candidate.checkout, path, inputs.carrier)
            .await?
            .ok_or(MountSourceError::NotFound)?;
        candidate
            .checkout
            .change_named_attribute_by_id(lookup.node.file_id, &name, value, work, cancellation)
            .await
            .map_err(fs_error)?;
        self.checkout
            .install_change(
                candidate,
                epoch,
                stamp,
                ViewChange::Node(lookup.node.file_id),
                work,
                &self.cancellation,
                self.native_callback_guard.clone(),
            )
            .await?;
        Ok(())
    }

    pub(super) async fn read_bytes(
        &self,
        path: &MountPath,
        range: Option<ByteRange>,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = carrier.cancellation();
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
        let mut checkout = inputs.observer(&observation, &cancellation)?;
        let chunk = if let Some(original) = inputs.carrier.original_native_work() {
            match range {
                Some(range) => {
                    let read = checkout
                        .read_file_range(&path, range, &original, &cancellation)
                        .await
                        .map_err(fs_error)?;
                    let bytes = read.logical_owned_bytes();
                    crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, bytes)
                }
                None => checkout
                    .read_symbolic_link(&path, &original, &cancellation)
                    .await
                    .map_err(fs_error)?,
            }
        } else {
            let (budget, live) = inputs.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(super::lazy::mount_demand_error)?,
                    work.live_allocation_bytes(),
                ))
            })?;
            let result = match range {
                Some(range) => checkout
                    .read_file_range(&path, range, budget, &cancellation)
                    .await
                    .map(|receipt| {
                        let bytes = receipt.value.logical_owned_bytes();
                        crate::OperationReceipt {
                            value: crate::kernel::OwnedSourceChunk::claimed(
                                receipt.value.bytes,
                                bytes,
                            ),
                            work: receipt.work,
                        }
                    }),
                None => {
                    checkout
                        .read_symbolic_link(&path, budget, &cancellation)
                        .await
                }
            };
            checkout.proof_data_take_completed_work();
            inputs
                .carrier
                .accept_with_prior_live(result, live, fs_error)?
        };
        inputs
            .carrier
            .finish_read(chunk, &cancellation, Some(&self.cancellation))
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
        let mut checkout = inputs.observer(&observation, &cancellation)?;
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

    pub(super) async fn record_by_id(
        &self,
        file_id: FileId,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<FileRecord, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = carrier.cancellation();
        let mut inputs = InputBuffers::new(carrier)?;
        let observation = self
            .checkout
            .observe(
                owner,
                inputs.carrier,
                &cancellation,
                Some(&self.cancellation),
            )
            .await?;
        observation.ensure_publication_resolved()?;
        let mut checkout = inputs.observer(&observation, &cancellation)?;
        let record = if let Some(original) = inputs.carrier.original_native_work() {
            checkout
                .read_file_record_by_id(file_id, &original, &cancellation)
                .await
                .map_err(fs_error)?
        } else {
            let (budget, live) = inputs.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(super::lazy::mount_demand_error)?,
                    work.live_allocation_bytes(),
                ))
            })?;
            let result = checkout
                .read_file_record_by_id(file_id, budget, &cancellation)
                .await;
            checkout.proof_data_take_completed_work();
            inputs
                .carrier
                .accept_with_prior_live(result, live, fs_error)?
        };
        inputs
            .carrier
            .check(&cancellation, Some(&self.cancellation))?;
        Ok(record)
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
            _native_allocation: None,
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
    pub fn new(
        checkout: Arc<SharedCheckout<A, O>>,
        config: VolumeConfig,
    ) -> Result<Self, NativeMountError> {
        let root = NamespacePath::new(Vec::new(), config.limits)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        Self::new_at(checkout, config, root)
    }

    /// Creates an adapter rooted at an authenticated workspace subtree.
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
            _namespace_allocation: None,
            _source_allocation: None,
        })
    }

    /// Whether `path` still names what it named after `stamp`, whatever
    pub(super) fn binding_unchanged_since(&self, path: &MountPath, stamp: ViewStamp) -> bool {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        self.checkout.view_gate.is_stable()
            && self.path(path).is_ok_and(|path| {
                self.checkout
                    .ledger
                    .binding_unchanged_since(&path, stamp, &mut work)
                    .unwrap_or(false)
            })
    }

    /// Returns the checkout's owning volume without filesystem I/O.
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
    pub fn sync(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        self.runtime
            .wait(|| self.sync_async(crate::PublicationPermit::Unrestricted))
    }

    /// Asynchronously publishes every pending mount mutation.
    pub async fn sync_async(&self, permit: crate::PublicationPermit) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let work = match &self.native_original {
            Some(work) => work.clone(),
            None => capture_meter(boundary_budget(), &self.cancellation)
                .map_err(|failure| failure.error)?,
        };
        self.checkout
            .publish(
                permit,
                &work,
                work.cancellation(),
                Some(&self.cancellation),
                &mut source_publication_guard(self.native_callback_guard.clone(), Some(self.cancellation.clone())),
                None,
                &mut None,
            )
            .await.map_err(|(cause, _)| cause)
    }

    /// Safely advances a clean mounted checkout to the workspace head.
    pub async fn refresh_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
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
    }

    /// Adopts the workspace head after this checkout's mutations were
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
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
        self.record_advance(&mut checkout, &base).await
    }

    /// Records exactly what advancing the checkout from `base` changed in
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

impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn change_content<F: crate::facade::ContentFile>(
        &self,
        file: F,
        change: ContentChange<F>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let (mut candidate, epoch, stamp) = self
                .checkout
                .candidate(
                    None,
                    &mut carrier,
                    work.cancellation(),
                    Some(&self.cancellation),
                )
                .await?;
            let node = match file.address() {
                crate::facade::ContentAddress::File(id) => id,
                crate::facade::ContentAddress::Path(path) => {
                    candidate
                        .checkout
                        .inspector(work, work.cancellation())?
                        .lookup_no_follow(path, work, work.cancellation())
                        .await?
                        .record
                        .ok_or(MountSourceError::NotFound)?
                        .file_id
                }
            };
            candidate
                .checkout
                .change_content(
                    file,
                    change,
                    ContentTimes::Stamp(content_change_time()?),
                    work,
                    work.cancellation(),
                )
                .await?;
            self.checkout
                .install_change(
                    candidate,
                    epoch,
                    stamp,
                    ViewChange::Node(node),
                    work,
                    &self.cancellation,
                    self.native_callback_guard.clone(),
                )
                .await?;
            Ok(())
        })
    }
}
