use super::*;

/// Immutable generation-fenced checkout admitted by the embedded facade.
pub struct Checkout<A, O> {
    pub(super) volume: Volume<A, O>,
    pub(super) generation_root: ObjectId,
    pub(super) base_root: GenerationRoot,
    /// The base generation and the working tree, which a running collection
    pub(super) root: crate::collection::CheckoutRoots,
    pub(super) authority_head: Option<Head>,
    pub(super) authored_operation_id: Option<OperationId>,
    pub(super) live_operation_id: Option<OperationId>,
    pub(super) last_commit: Option<LastCommit>,
    pub(super) publication_closure: Option<PublicationClosure<A, O>>,
    pub(super) prepared_merge_parent: Option<ObjectId>,
    pub(super) dependencies: DependencyLedger,
    /// Exact newly owned proof payload admitted when this handle was constructed.
    pub(super) proof_owned_bytes: u64,
    pub(super) mode: CheckoutMode,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Volume<A, O> {
    /// Opens one authenticated checkout under the supplied work limits.
    pub async fn checkout<W: FsWork>(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Checkout<A, O>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            accounting.check()?;
            validate_checkout(mode, self.config).map_err(|failure| failure.error)?;
            let (generation_root, mut authority_head) = self
                .resolve_checkout_generation(selector, mode, &mut accounting, cancellation)
                .await?;
            let before = readonly_standalone_live(&accounting);
            let (root, allocation) = read_generation_root_with_accounting(
                &self.fs.inner.objects,
                generation_root,
                self.config,
                &mut accounting,
                cancellation,
            )
            .await?;
            let root_bytes = root_parent_bytes(&root)?;
            readonly_cleanup_standalone(&mut accounting, before, root_bytes)?;
            if root.volume_id != self.id {
                return Err(FsError::VolumeMismatch);
            }
            if mode.access == AccessMode::ReadWrite
                && self.config.concurrency == ConcurrencyMode::ExclusiveWriter
            {
                let expected = authority_head.ok_or(FsError::WritableCheckoutRequiresHead)?;
                let mut work = authority_work(&accounting, cancellation)?;
                let result = self
                    .fs
                    .inner
                    .authority
                    .fence(volume_authority_id(self.id), expected, &mut work)
                    .await;
                record_authority_work(&mut accounting, &work)?;
                authority_head = Some(match result? {
                    FenceOutcome::Advanced(head) => head,
                    FenceOutcome::Conflict { actual } => {
                        return Err(FsError::ExclusiveWriterConflict { actual });
                    }
                });
            }
            let custody = Checkout::<A, O>::checkout_custody(&mut accounting, cancellation)?;
            let captured_bytes = usize_to_u64(CheckoutDependencies::empty_logical_owned_bytes());
            let mut captured_allocation = accounting.claim_owned(captured_bytes, 1)?;
            let captured = CheckoutDependencies::new(
                std::iter::empty(),
                std::iter::empty(),
                self.config.limits.maximum_checkout_dependencies,
            )?;
            let proof_bytes = usize_to_u64(size_of::<Mutex<Proof>>());
            let mut proof_allocation = accounting.claim_owned(proof_bytes, 1)?;
            let (base_root, root, dependencies) = match allocation {
                #[cfg(not(target_arch = "wasm32"))]
                KernelOwnedAllocation::NativeOriginal(mut root_allocation) => {
                    let original = accounting
                        .native_work()
                        .expect("native checkout allocation");
                    let witness = original.checkout_control_sink(&custody)?;
                    let working = root_allocation.transfer_checkout_control(&custody, &witness)?;
                    let mut captured_control = native_pinned_allocation(captured_allocation)?
                        .transfer_checkout_control(&custody, &witness)?;
                    let mut captured = captured;
                    assert!(
                        captured.attach_fresh_paid_control(&mut captured_control),
                        "fresh checkout owns its dependency map"
                    );
                    let proof_control = native_pinned_allocation(proof_allocation)?
                        .transfer_checkout_control(&custody, &witness)?;
                    let mut dependencies =
                        DependencyLedger::new_with_native_custody(captured, custody, proof_control);
                    let (base, control) = crate::collection::clone_generation_root_native(
                        &root,
                        &mut accounting,
                        &dependencies.data_custody,
                    )?;
                    dependencies.base_root_control = control;
                    let root = crate::collection::CheckoutRoots::new_native_measured(
                        self.fs.inner.objects.collection(),
                        self.config,
                        generation_root,
                        (root, working),
                        &mut accounting,
                        &dependencies.data_custody,
                    )?;
                    (base, root, dependencies)
                }
                KernelOwnedAllocation::Standalone => {
                    drop((captured_allocation, proof_allocation));
                    let mut dependencies =
                        DependencyLedger::sharing_with_custody(Proof::new(captured), custody);
                    dependencies.proof().control =
                        ProofDataClaim::control(&dependencies.data_custody, proof_bytes);
                    assert!(
                        dependencies
                            .proof()
                            .dependencies
                            .attach_fresh_control_custody(&dependencies.data_custody),
                        "fresh checkout owns its dependency map"
                    );
                    let (base, _allocation) =
                        crate::collection::clone_generation_root_with_accounting(
                            &root,
                            &mut accounting,
                        )?;
                    dependencies.base_root_control = ProofDataClaim::control(
                        &dependencies.data_custody,
                        root_parent_bytes(&base)?,
                    );
                    let (allocations, work, allowance, _) =
                        accounting.standalone_parts().expect("standalone checkout");
                    let mut root = crate::collection::CheckoutRoots::new_measured(
                        self.fs.inner.objects.collection(),
                        self.config,
                        generation_root,
                        root,
                        allocations,
                        work,
                        allowance,
                        cancellation,
                    )?;
                    root.attach_control_custody(&dependencies.data_custody);
                    (base, root, dependencies)
                }
            };
            let proof_owned_bytes = captured_bytes
                .checked_add(proof_bytes)
                .and_then(|bytes| bytes.checked_add(size_of::<ProofDataCustody>() as u64))
                .ok_or(WorkError::Overflow)?;
            accounting.check()?;
            Ok(Checkout {
                volume: self.clone(),
                generation_root,
                base_root,
                root,
                authority_head,
                authored_operation_id: None,
                live_operation_id: None,
                last_commit: None,
                publication_closure: None,
                prepared_merge_parent: None,
                dependencies,
                proof_owned_bytes,
                mode,
            })
        }
        .await;
        budget.finish(result, &accounting)
    }
}

impl<A, O> Checkout<A, O> {
    /// Owning volume identity.
    #[must_use]
    pub const fn volume_id(&self) -> VolumeId {
        self.volume.id
    }

    /// Immutable owning-volume semantics and operation bounds.
    #[must_use]
    pub const fn volume_config(&self) -> VolumeConfig {
        self.volume.config
    }

    /// Exact immutable generation identity.
    #[must_use]
    pub const fn generation_id(&self) -> GenerationId {
        GenerationId::new(self.generation_root.digest)
    }

    /// Authenticated decoded generation root.
    #[must_use]
    pub fn root(&self) -> &GenerationRoot {
        &self.root
    }

    /// Admitted checkout behavior.
    #[must_use]
    pub const fn mode(&self) -> CheckoutMode {
        self.mode
    }

    /// Whether the private candidate differs from its immutable base.
    #[must_use]
    pub fn has_pending_mutations(&self) -> bool {
        self.root.file_table != self.base_root.file_table || self.prepared_merge_parent.is_some()
    }

    /// Whether reads retain exact observations for safe rebase and commit.
    pub(super) fn tracks_observations(&self) -> bool {
        self.mode.consistency != ConsistencyMode::Pinned || self.dependencies.observing
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Discards a private candidate and reauthenticates its immutable base.
    pub async fn discard(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        if self.mode.mutations == MutationMode::None {
            return Err(OperationFailure::before_work(FsError::MutationNotAllowed));
        }
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            self.reset_root(self.root.base(), &mut accounting, cancellation)
                .await?;
            self.live_operation_id = None;
            self.prepared_merge_parent = None;
            Ok(())
        }
        .await;
        measured(result, &accounting)
    }

    /// Explicitly advances one clean manual or tracking checkout to head.
    pub async fn refresh_head(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationId> {
        if !matches!(
            self.mode.consistency,
            ConsistencyMode::Manual | ConsistencyMode::TrackingSafe
        ) {
            return Err(OperationFailure::before_work(FsError::RefreshNotAllowed));
        }
        if self.has_pending_mutations() {
            return Err(OperationFailure::before_work(
                FsError::PendingMutationsRequireRebase,
            ));
        }
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let (root, head) = self
                .volume
                .resolve_head_generation(&mut accounting, cancellation)
                .await?;
            if root != self.generation_root {
                self.reset_root(root, &mut accounting, cancellation).await?;
            }
            self.authority_head = Some(head);
            Ok(self.generation_id())
        }
        .await;
        measured(result, &accounting)
    }

    async fn reset_root(
        &mut self,
        generation_root: ObjectId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let before = readonly_standalone_live(accounting);
        let (root, allocation) = read_generation_root_with_accounting(
            &self.volume.fs.inner.objects,
            generation_root,
            self.volume.config,
            accounting,
            cancellation,
        )
        .await?;
        readonly_cleanup_standalone(accounting, before, root_parent_bytes(&root)?)?;
        if root.volume_id != self.volume.id {
            return Err(FsError::VolumeMismatch);
        }
        let prepared =
            self.prepare_root_replacement(&root, &root, accounting, cancellation, None)?;
        self.install_root_replacement(generation_root, root, prepared);
        drop(allocation);
        self.generation_root = generation_root;
        self.dependencies.proof().clear();
        Ok(())
    }

    /// Checks retained original custody, without admitting work or granting rights.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn validate_native_work(
        &self,
        original: &crate::NativeMaterializationWork,
    ) -> Result<(), FsError> {
        if !original.owns_native_checkout_token(&self.dependencies.data_custody)
            || !self.root.same_original_scope(&original.state)
            || !self
                .dependencies
                .base_root_control
                .is_native_original(&original.state, root_parent_bytes(&self.base_root)?)
        {
            return Err(FsError::NativeKernel(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        Ok(())
    }
}
