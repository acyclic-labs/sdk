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

    pub(super) fn namespace_path(
        &self,
        path: &MountPath,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(NamespacePath, Option<crate::NativeAllocation>), MountSourceError> {
        let mut inputs = InputBuffers::new(carrier)?;
        let path = self.namespace_inputs(path, &mut inputs)?;
        Ok((path, inputs.retain()))
    }

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
        action: crate::access::Action,
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
        let work = inputs
            .carrier
            .original_native_work()
            .ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?;
        let permissions = self.require_access(&path, action, &work).await?;
        self.disclose(permissions.policy_view(), &work, async {
            let mut inspector = checkout.inspector(
                &mut crate::kernel::allocation::KernelAccounting::native(&work, &cancellation),
                &cancellation,
            )?;
            self.require_snapshot(permissions.policy_view(), &mut inspector, &work)
                .await?;
            self.read_lookup(&mut inspector, &path, &mut inputs, &cancellation)
                .await
        })
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
        let name = inputs.mutation_attribute_name(name, self.config.profile, self.config.limits)?;
        let observation = self
            .checkout
            .observe(
                ViewGate::callback_owner()?,
                inputs.carrier,
                cancellation,
                Some(&self.cancellation),
            )
            .await?;
        let file = self
            .lookup_in(
                &observation,
                path,
                crate::access::Action::Write,
                inputs.carrier,
            )
            .await?
            .ok_or(MountSourceError::NotFound)?
            .node
            .file_id;
        drop(observation);
        let mut candidate = self
            .checkout
            .file_candidate(file, inputs.carrier, cancellation, Some(&self.cancellation))
            .await?;
        let lookup = self
            .lookup_in(
                &candidate.checkout,
                path,
                crate::access::Action::Write,
                inputs.carrier,
            )
            .await?
            .ok_or(MountSourceError::NotFound)?;
        if lookup.node.file_id != file {
            return Err(MountSourceError::Stale);
        }
        candidate.deadline = self.mutation_deadline()?;
        candidate
            .checkout
            .change_named_attribute(lookup.node.file_id, &name, value, work, cancellation)
            .await?;
        self.checkout
            .install_change(
                candidate,
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
        let work = inputs
            .carrier
            .original_native_work()
            .ok_or(MountSourceError::AccessDenied)?;
        let permissions = self
            .require_access(&path, crate::access::Action::Read, &work)
            .await?;
        self.disclose(permissions.policy_view(), &work, async {
            let observation = self
                .checkout
                .observe(
                    owner,
                    inputs.carrier,
                    &cancellation,
                    Some(&self.cancellation),
                )
                .await?;
            let original =
                inputs
                    .carrier
                    .original_native_work()
                    .ok_or(MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ))?;
            let mut checkout = observation.observer(
                &mut crate::kernel::allocation::KernelAccounting::native(&original, &cancellation),
                &cancellation,
            )?;
            self.require_snapshot(permissions.policy_view(), &mut checkout, &work)
                .await?;
            let chunk = match range {
                Some(range) => {
                    let read = checkout
                        .read_file_range(&path, range, &original, &cancellation)
                        .await?;
                    let bytes = read.logical_owned_bytes();
                    crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, bytes)
                }
                None => {
                    checkout
                        .read_symbolic_link(&path, &original, &cancellation)
                        .await?
                }
            };
            inputs
                .carrier
                .check(&cancellation, Some(&self.cancellation))?;
            Ok(chunk)
        })
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
        self.lookup_identity(path, owner, carrier, true).await
    }

    pub(in crate::native_mount) async fn lookup_identity(
        &self,
        path: &MountPath,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
        disclose: bool,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = carrier.cancellation();
        carrier.check(&cancellation, Some(&self.cancellation))?;
        let mut inputs = InputBuffers::new(carrier)?;
        let path = self.namespace_inputs(path, &mut inputs)?;
        let work = inputs
            .carrier
            .original_native_work()
            .ok_or(MountSourceError::AccessDenied)?;
        let access = self.pin_access(&path, &work).await?;
        if disclose
            && !access
                .decide(
                    crate::access::Action::List,
                    access::access_time(&self.runtime.clock)?,
                )
                .allowed
        {
            return Ok(None);
        }
        self.disclose(access.policy_view(), &work, async {
            let observation = self
                .checkout
                .observe(
                    owner,
                    inputs.carrier,
                    &cancellation,
                    Some(&self.cancellation),
                )
                .await?;
            let original =
                inputs
                    .carrier
                    .original_native_work()
                    .ok_or(MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ))?;
            let mut checkout = observation.observer(
                &mut crate::kernel::allocation::KernelAccounting::native(&original, &cancellation),
                &cancellation,
            )?;
            self.require_snapshot(access.policy_view(), &mut checkout, &work)
                .await?;
            self.read_lookup(&mut checkout, &path, &mut inputs, &cancellation)
                .await
        })
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
        let work = inputs
            .carrier
            .original_native_work()
            .ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?;
        if self.folds_names() {
            let _ = self
                .require_unique_folded_path(checkout, path, false, &work, cancellation)
                .await?;
        }
        let value = checkout
            .lookup_no_follow_with_metadata(path, &work, cancellation)
            .await?;
        inputs
            .carrier
            .check(cancellation, Some(&self.cancellation))?;
        if value.is_some() {
            inputs.charge(
                crate::materializer::NativeResourceCredit::Copy
                    .delta(std::mem::size_of::<MountLookup>() as u64),
            )?;
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
        let observation = self
            .checkout
            .observe(owner, carrier, &cancellation, Some(&self.cancellation))
            .await?;
        observation.ensure_publication_resolved()?;
        let original = carrier
            .original_native_work()
            .ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?;
        let mut checkout = observation.observer(
            &mut crate::kernel::allocation::KernelAccounting::native(&original, &cancellation),
            &cancellation,
        )?;
        let record = checkout
            .read_file_record_by_id(file_id, &original, &cancellation)
            .await?;
        carrier.check(&cancellation, Some(&self.cancellation))?;
        Ok(record)
    }

    pub(super) async fn detached_file_from_record(
        &self,
        record: FileRecord,
        metadata: FileMetadata,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Arc<CheckoutDetachedFile<A, O>>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = carrier.cancellation();
        let checkout = self
            .checkout
            .observe(owner, carrier, &cancellation, Some(&self.cancellation))
            .await?;
        let work = carrier
            .original_native_work()
            .ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?;
        self.detached_from_checkout(&checkout, record, metadata, &work)
    }

    fn detached_from_checkout(
        &self,
        checkout: &Checkout<A, O>,
        record: FileRecord,
        metadata: FileMetadata,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Arc<CheckoutDetachedFile<A, O>>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let allocation = work
            .claim(
                std::mem::size_of::<CheckoutDetachedFile<A, O>>() as u64,
                1,
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(MountSourceError::NativeWork)?;
        let file = checkout.detached_from_record(record, &work, work.cancellation())?;
        NativeDemandCarrier::Native(work).check(work.cancellation(), Some(&self.cancellation))?;
        Ok(Arc::new(CheckoutDetachedFile {
            _native_allocation: allocation,
            state: tokio::sync::Mutex::new(DetachedMountState {
                file,
                metadata,
                mutation_epoch: 0,
            }),
            ledger: Arc::clone(&self.checkout.ledger),
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.config.profile,
            limits: self.config.limits,
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
        if config != checkout.config {
            return Err(MountSourceError::AccessDenied.into());
        }
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
            config,
            access_view: None,
            access_refresh: None,
            folds_names: config.case_sensitivity == CaseSensitivity::ProfileFolded
                && matches!(
                    config.profile,
                    FilesystemProfile::Portable | FilesystemProfile::Browser
                ),
            folded_directories: folded_cache::FoldedDirectoryCache::new(),
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

    /// Returns the checkout's owning volume without filesystem I/O.
    pub fn volume_id(&self) -> Result<VolumeId, MountSourceError> {
        self.cancellation
            .check()
            .map_err(|_| MountSourceError::Stale)?;
        Ok(self.checkout.volume_id)
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
            None => capture_meter(BOUNDARY_BUDGET, &self.cancellation)?,
        };
        self.checkout
            .publish(
                permit,
                crate::kernel::KernelAccounting::native(&work, work.cancellation()),
                Some(&self.cancellation),
                &mut source_publication_guard(
                    self.native_callback_guard.clone(),
                    Some(self.cancellation.clone()),
                ),
                None,
                &mut None,
            )
            .await
            .map_err(|(cause, _)| cause)
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
            .private_candidate(
                &mut crate::kernel::allocation::KernelAccounting::standalone(
                    BOUNDARY_BUDGET,
                    &self.cancellation,
                ),
                &self.cancellation,
            )
            .map_err(MountSourceError::from)?;
        let decision = checkout
            .rebase_head(
                self.config.limits.maximum_checkout_dependencies,
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
            .private_candidate(
                &mut crate::kernel::allocation::KernelAccounting::standalone(
                    BOUNDARY_BUDGET,
                    &self.cancellation,
                ),
                &self.cancellation,
            )
            .map_err(MountSourceError::from)?;
        checkout
            .refresh_head(WorkBudget::UNBOUNDED, &self.cancellation)
            .await
            .map_err(engine_error)?;
        self.record_advance(&mut checkout, &base).await
    }

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
        let root = NamespacePath::new(Vec::new(), self.config.limits)
            .map_err(MountSourceError::NamespacePath)?;
        let work = capture_meter(BOUNDARY_BUDGET, &self.cancellation)?;
        let (installed, _allocation) = installed_changes(
            base,
            checkout,
            InstallScope::Subtree(&root),
            &mut NativeDemandCarrier::Native(&work),
            &self.cancellation,
        )
        .await?;
        self.checkout.view_gate.begin_transition();
        checkout.record(&ViewChange::Effect(&installed))?;
        self.checkout.view_gate.finish_transition();
        Ok(())
    }
}

impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn change_content(
        &self,
        file: NamespacePath,
        change: ContentChange<NamespacePath>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let view = self.access_snapshot(work).await?;
            let now = access::access_time(&self.runtime.clock)?;
            let permissions = view
                .pin(&file)
                .map_err(|_| MountSourceError::AccessDenied)?;
            if let ContentChange::CloneFrom { source, .. } = &change {
                access::require(
                    &view
                        .pin(source)
                        .map_err(|_| MountSourceError::AccessDenied)?,
                    crate::access::Action::Read,
                    now,
                )?;
            }
            let append_only = !permissions
                .decide(crate::access::Action::Write, now)
                .allowed;
            if append_only {
                let action = if matches!(&change, ContentChange::Write { .. }) {
                    crate::access::Action::Append
                } else {
                    crate::access::Action::Write
                };
                access::require(&permissions, action, now)?;
            }
            self.checkout
                .change_file_content(
                    file,
                    change,
                    append_only,
                    ContentChangeContext {
                        work,
                        deadline: view.valid_until(),
                        lifetime: &self.cancellation,
                        clock: &self.runtime.clock,
                        guard: self.native_callback_guard.clone(),
                    },
                )
                .await
        })
    }
}
