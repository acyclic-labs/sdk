use super::*;

/// Immutable generation-fenced checkout admitted by the embedded facade.
pub struct Checkout<A, O> {
    pub(super) volume: Volume<A, O>,
    pub(super) generation_root: ObjectId,
    pub(super) base_file_table: ObjectId,
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
    /// Opens one immutable pinned checkout after authenticating its bounded root.
    pub async fn checkout(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Checkout<A, O>> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.checkout",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
            volume_id = crate::obs::hex(&self.id.into_bytes()),
        );
        let result = crate::obs::in_span(&span, async move {
            cancellation
                .check()
                .map_err(|error| OperationFailure::before_work(error.into()))?;
            validate_checkout(mode, self.config)?;
            let mut work = WorkCounters::default();
            let (generation_root, mut authority_head, next_work) = self
                .resolve_checkout_generation(selector, mode, work, budget, cancellation)
                .await?;
            work = next_work;
            let (root, root_work) = read_generation_root(
                &self.fs.inner.objects,
                generation_root,
                self.config,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
            work = add(work, root_work)?;
            if root.volume_id != self.id {
                return Err(OperationFailure::new(FsError::VolumeMismatch, work));
            }
            if mode.access == AccessMode::ReadWrite
                && self.config.concurrency == ConcurrencyMode::ExclusiveWriter
            {
                let expected = authority_head.ok_or_else(|| {
                    OperationFailure::new(FsError::WritableCheckoutRequiresHead, work)
                })?;
                let fenced = ({
                    let __authority_args = (
                        volume_authority_id(self.id),
                        expected,
                        remaining(work, budget)?,
                        cancellation,
                    );
                    let mut __authority_work =
                        crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
                    let __authority_result = self
                        .fs
                        .inner
                        .authority
                        .fence(
                            __authority_args.0,
                            __authority_args.1,
                            &mut __authority_work,
                        )
                        .await;
                    __authority_work.receipt(__authority_result)
                })
                .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
                work = add(work, fenced.work)?;
                authority_head = Some(match fenced.value {
                    FenceOutcome::Advanced(head) => head,
                    FenceOutcome::Conflict { actual } => {
                        return Err(OperationFailure::new(
                            FsError::ExclusiveWriterConflict { actual },
                            work,
                        ));
                    }
                });
            }
            let mut allocations = AllocationLedger::default();
            allocations
                .claim_bytes(
                    usize_to_u64(root.parents.capacity() * size_of::<GenerationId>()),
                    0,
                    &mut work,
                    budget,
                )
                .map_err(|error| {
                    OperationFailure::new(crate::collection::handle_allocation_error(error), work)
                })?;
            let captured_bytes = usize_to_u64(CheckoutDependencies::empty_logical_owned_bytes());
            allocations
                .claim_bytes(captured_bytes, 1, &mut work, budget)
                .map_err(|error| {
                    OperationFailure::new(crate::collection::handle_allocation_error(error), work)
                })?;
            let dependencies = CheckoutDependencies::new(
                std::iter::empty(),
                std::iter::empty(),
                self.config.limits.maximum_checkout_dependencies,
            )
            .map_err(|error| OperationFailure::new(error.into(), work))?;
            let lock_and_paths_bytes =
                usize_to_u64(size_of::<Mutex<Proof>>() + size_of::<ObservedPathMap>());
            allocations
                .claim_bytes(lock_and_paths_bytes, 2, &mut work, budget)
                .map_err(|error| {
                    OperationFailure::new(crate::collection::handle_allocation_error(error), work)
                })?;
            let custody_bytes = allocations
                .claim_elements::<ProofDataCustody>(1, &mut work, budget)
                .map_err(|error| {
                    OperationFailure::new(crate::collection::handle_allocation_error(error), work)
                })?;
            let mut dependencies = DependencyLedger::new(dependencies);
            dependencies.proof().control = ProofDataClaim::control(
                &dependencies.data_custody,
                usize_to_u64(size_of::<Mutex<Proof>>()),
            );
            assert!(
                dependencies
                    .proof()
                    .dependencies
                    .attach_fresh_control_custody(&dependencies.data_custody,),
                "a freshly constructed checkout owns its unique empty dependency map"
            );
            Arc::get_mut(
                dependencies
                    .proof()
                    .observed_paths
                    .as_mut()
                    .expect("a fresh checkout owns its observed map"),
            )
            .expect("a freshly constructed checkout owns its unique empty observed map")
            .control = ProofDataClaim::control(
                &dependencies.data_custody,
                usize_to_u64(size_of::<ObservedPathMap>()),
            );
            let proof_owned_bytes = captured_bytes
                .checked_add(lock_and_paths_bytes)
                .and_then(|bytes| bytes.checked_add(custody_bytes))
                .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
            let base_root = crate::collection::clone_generation_root(
                &root,
                &mut allocations,
                &mut work,
                budget,
                &cancellation,
            )
            .map_err(|error| OperationFailure::new(error, work))?;
            dependencies.base_root_control = ProofDataClaim::control(
                &dependencies.data_custody,
                root_parent_bytes(&base_root)
                    .map_err(|error| OperationFailure::new(error, work))?,
            );
            let mut root = crate::collection::CheckoutRoots::new_measured(
                self.fs.inner.objects.collection(),
                self.config,
                generation_root,
                root,
                &mut allocations,
                &mut work,
                budget,
                cancellation,
            )
            .map_err(|error| OperationFailure::new(error, work))?;
            root.attach_control_custody(&dependencies.data_custody);
            Ok(FsReceipt {
                value: Checkout {
                    volume: self.clone(),
                    generation_root,
                    base_file_table: root.file_table,
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
                },
                work,
            })
        })
        .await;
        result.observe_on(&span, "checkout")
    }

    /// Opens the authenticated checkout on the supplied original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn checkout_with_native_work(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        original: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Checkout<A, O>, FsError> {
        cancellation.check()?;
        validate_checkout(mode, self.config).map_err(|failure| failure.error)?;
        let (generation_root, mut authority_head) = self
            .resolve_checkout_generation_with_native_work(selector, mode, original, cancellation)
            .await?;
        let mut accounting = KernelAccounting::native(original, cancellation);
        let decoded = read_generation_root_with_accounting(
            &self.fs.inner.objects,
            generation_root,
            self.config,
            &mut accounting,
            cancellation,
        )
        .await?;
        let mut root_guard = match decoded.1 {
            KernelOwnedAllocation::NativeOriginal(guard) => guard,
            KernelOwnedAllocation::Standalone => {
                unreachable!("native root accounting returns its original allocation guard")
            }
        };
        let root = decoded.0;
        if root.volume_id != self.id {
            return Err(FsError::VolumeMismatch);
        }
        if mode.access == AccessMode::ReadWrite
            && self.config.concurrency == ConcurrencyMode::ExclusiveWriter
        {
            let expected = authority_head.ok_or(FsError::WritableCheckoutRequiresHead)?;
            authority_head = Some(
                match ({
                    let __authority_args = (
                        volume_authority_id(self.id),
                        expected,
                        original,
                        cancellation,
                    );
                    let mut __authority_work =
                        crate::AuthorityWork::native(__authority_args.2, __authority_args.3);
                    let __authority_result = self
                        .fs
                        .inner
                        .authority
                        .fence(
                            __authority_args.0,
                            __authority_args.1,
                            &mut __authority_work,
                        )
                        .await;
                    __authority_result
                })? {
                    FenceOutcome::Advanced(head) => head,
                    FenceOutcome::Conflict { actual } => {
                        return Err(FsError::ExclusiveWriterConflict { actual });
                    }
                },
            );
        }
        cancellation.check()?;
        let mut token_header =
            original.claim_root_metadata(std::mem::size_of::<ProofDataCustody>() as u64)?;
        let custody_bytes = token_header.bytes();
        let custody = Arc::new(ProofDataCustody::default());
        let witness = original.register_native_checkout_control(&custody, &mut token_header)?;
        let working_control = root_guard.transfer_checkout_control(&custody, &witness)?;
        let working = (root, working_control);
        let captured_bytes = usize_to_u64(CheckoutDependencies::empty_logical_owned_bytes());
        cancellation.check()?;
        let mut captured_guard =
            original.claim(captured_bytes, 1, crate::WorkCounters::UNCHARGED)?;
        let mut captured_control = captured_guard.transfer_checkout_control(&custody, &witness)?;
        let mut captured = CheckoutDependencies::new(
            std::iter::empty(),
            std::iter::empty(),
            self.config.limits.maximum_checkout_dependencies,
        )?;
        assert!(
            captured.attach_fresh_paid_control(&mut captured_control),
            "a fresh checkout owns its unique empty dependency map"
        );
        let proof_bytes = usize_to_u64(size_of::<Mutex<Proof>>());
        let paths_bytes = usize_to_u64(size_of::<ObservedPathMap>());
        cancellation.check()?;
        let mut proof_guard = original.claim(proof_bytes, 1, crate::WorkCounters::UNCHARGED)?;
        let mut paths_guard = original.claim(paths_bytes, 1, crate::WorkCounters::UNCHARGED)?;
        let proof_control = proof_guard.transfer_checkout_control(&custody, &witness)?;
        let paths_control = paths_guard.transfer_checkout_control(&custody, &witness)?;
        let mut dependencies = DependencyLedger::new_with_native_custody(
            captured,
            custody,
            proof_control,
            paths_control,
        );
        let (base_root, base_control) = crate::collection::clone_generation_root_native(
            &working.0,
            &mut accounting,
            &dependencies.data_custody,
        )?;
        dependencies.base_root_control = base_control;
        let root = crate::collection::CheckoutRoots::new_native_measured(
            self.fs.inner.objects.collection(),
            self.config,
            generation_root,
            working,
            &mut accounting,
            &dependencies.data_custody,
        )?;
        let proof_owned_bytes = captured_bytes
            .checked_add(proof_bytes)
            .and_then(|bytes| bytes.checked_add(paths_bytes))
            .and_then(|bytes| bytes.checked_add(custody_bytes))
            .ok_or(WorkError::Overflow)?;
        accounting.check()?;
        Ok(Checkout {
            volume: self.clone(),
            generation_root,
            base_file_table: root.file_table,
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
        self.root.file_table != self.base_file_table || self.prepared_merge_parent.is_some()
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
        let (root, mut work) = read_generation_root(
            &self.volume.fs.inner.objects,
            self.root.base(),
            self.volume.config,
            budget,
            cancellation,
        )
        .await?;
        if root.volume_id != self.volume.id {
            return Err(OperationFailure::new(FsError::VolumeMismatch, work));
        }
        let live = root_parent_bytes(&root).map_err(|error| OperationFailure::new(error, work))?;
        let mut root_accounting =
            KernelAccounting::standalone(remaining_owned(work, budget, live)?, cancellation);
        let prepared = measured(
            self.prepare_root_replacement(&root, &root, &mut root_accounting, cancellation, None),
            &root_accounting,
        )
        .map_err(|failure| merge_simultaneous_failure(work, *failure.work, live, failure.error))?;
        work = merge_simultaneous_work(work, prepared.work, live, budget)?;
        self.install_root_replacement(self.root.base(), root, prepared.value);
        self.base_file_table = self.root.file_table;
        self.generation_root = self.root.base();
        self.live_operation_id = None;
        self.prepared_merge_parent = None;
        self.dependencies.proof().clear();
        Ok(FsReceipt { value: (), work })
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
        let (generation_root, head, mut work) = self
            .volume
            .resolve_head_generation(budget, cancellation)
            .await?;
        if generation_root == self.generation_root {
            self.authority_head = Some(head);
            return Ok(FsReceipt {
                value: self.generation_id(),
                work,
            });
        }
        let (root, root_work) = read_generation_root(
            &self.volume.fs.inner.objects,
            generation_root,
            self.volume.config,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, root_work)?;
        if root.volume_id != self.volume.id {
            return Err(OperationFailure::new(FsError::VolumeMismatch, work));
        }
        let live = root_parent_bytes(&root).map_err(|error| OperationFailure::new(error, work))?;
        let mut root_accounting =
            KernelAccounting::standalone(remaining_owned(work, budget, live)?, cancellation);
        let prepared = measured(
            self.prepare_root_replacement(&root, &root, &mut root_accounting, cancellation, None),
            &root_accounting,
        )
        .map_err(|failure| merge_simultaneous_failure(work, *failure.work, live, failure.error))?;
        work = merge_simultaneous_work(work, prepared.work, live, budget)?;
        self.generation_root = generation_root;
        self.install_root_replacement(generation_root, root, prepared.value);
        self.base_file_table = self.root.file_table;
        self.authority_head = Some(head);
        self.dependencies.proof().clear();
        Ok(FsReceipt {
            value: self.generation_id(),
            work,
        })
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
