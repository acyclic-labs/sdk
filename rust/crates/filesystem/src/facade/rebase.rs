use super::*;

pub(super) struct RebaseAdoption<'a> {
    pub(super) object: ObjectId,
    pub(super) head: Head,
    pub(super) classified: Decided,
    pub(super) root: Owned<GenerationRoot>,
    pub(super) prepared: super::candidates::PreparedCheckoutRoots,
    pub(super) settled: Option<crate::kernel::Settled<'a>>,
    pub(super) certificate: Option<super::rebase_footprint::DisjointChanges>,
}

struct ClassifiedRebase<'a> {
    object: ObjectId,
    head: Head,
    decision: Decided,
    settled: Option<crate::kernel::Settled<'a>>,
    certificate: Option<super::rebase_footprint::DisjointChanges>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn live_rebase_workspace(
        &self,
        request: WorkspaceRebaseRequest<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<WorkspaceJoinOutcome>, WorkspaceError> {
        let target = request.target;
        let prepared = self
            .prepare_live_rebase(request, accounting, cancellation)
            .await?;
        self.publish_workspace_join_prepared(target, prepared, accounting, cancellation)
            .await
    }

    async fn prepare_live_rebase(
        &self,
        request: WorkspaceRebaseRequest<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::workspace::JoinPreparation, WorkspaceError> {
        let mut scope = accounting.allocation_scope()?;
        let accounting = &mut *scope;
        accounting.check()?;
        let WorkspaceRebaseRequest {
            target,
            operation_id,
            maximum_generations,
            maximum_changes,
            maximum_conflicts,
        } = request;
        if maximum_generations == 0 || maximum_changes == 0 || maximum_conflicts == 0 {
            return Err(WorkspaceError::JoinLimit);
        }
        let RebaseLineage {
            target_object,
            target_head: expected_head,
            target_root,
            base_object,
            mut base_root,
            source,
            source_object,
        } = self
            .rebase_lineage(target, maximum_generations, accounting, cancellation)
            .await?;
        let result = async {
            if source_object == base_object {
                return super::join_request::workspace_join_scalar(
                    WorkspaceJoinOutcome::NoChanges(GenerationId::from_root(target_object)),
                    accounting,
                )
                .map(crate::workspace::JoinPreparation::Complete);
            }
            let mut source_root = self
                .read_generation_owned_with_accounting(
                    source_object,
                    target.config,
                    accounting,
                    cancellation,
                )
                .await?;
            let result = async {
                if source_root.value.volume_id != source.id
                    || source_root.value.root_file_id != target_root.value.root_file_id
                {
                    return Err(WorkspaceError::IncompatibleWorkspace);
                }
                // Normalize in place while each decode retains its original backing.
                for root in [&mut base_root.value, &mut source_root.value] {
                    root.volume_id = target.id;
                    root.required_features = root.continuation_features();
                    root.parents.clear();
                }
                let normalized_base_object = self
                    .put_generation_root_with_accounting(&base_root.value, accounting, cancellation)
                    .await?;
                let normalized_source_object = self
                    .put_generation_root_with_accounting(
                        &source_root.value,
                        accounting,
                        cancellation,
                    )
                    .await?;
                let request = crate::kernel::MergeGenerationBorrowedRequest {
                    base_generation: normalized_base_object,
                    base: &base_root.value,
                    ours_generation: Some(normalized_source_object),
                    ours: &source_root.value,
                    theirs_generation: target_object,
                    theirs: &target_root.value,
                    retain_theirs_parent: false,
                    maximum_changes,
                    maximum_conflicts,
                    content: crate::kernel::ContentMerge::Extents,
                };
                let (merged, allocation) =
                    merge_generation_with_borrowed_map_resolutions_with_accounting(
                        &self.inner.objects,
                        &request,
                        &BTreeMap::new(),
                        DecodeLimits::for_volume(target.config),
                        accounting,
                        cancellation,
                    )
                    .await?;
                let mut root = match merged {
                    MergeGenerationOutcome::Conflicted {
                        conflicts,
                        truncated,
                    } => {
                        let mut output = Owned::new(
                            WorkspaceJoinOutcome::Conflicted(conflicts, truncated),
                            allocation,
                            0,
                        );
                        if let WorkspaceJoinOutcome::Conflicted(conflicts, _) = &output.value {
                            output.bytes =
                                super::join_request::workspace_join_conflict_bytes(conflicts)?;
                        }
                        return Ok(crate::workspace::JoinPreparation::Complete(output));
                    }
                    MergeGenerationOutcome::Prepared { root, .. } => {
                        let bytes = root_parent_bytes(&root)?;
                        Owned::new(root, allocation, bytes)
                    }
                };
                let candidate = async {
                    let parent = root.value.parents.first_mut().ok_or(FsError::InvalidDiff)?;
                    let copied = WorkCounters {
                        bytes_copied: size_of::<GenerationId>() as u64,
                        ..WorkCounters::UNCHARGED
                    };
                    accounting.admit(copied)?;
                    *parent = GenerationId::from_root(source_object);
                    accounting.record_completed(copied)?;
                    self.put_generation_root_with_accounting(&root.value, accounting, cancellation)
                        .await
                }
                .await;
                root.retire(accounting)?;
                let candidate = candidate?;
                Ok(crate::workspace::JoinPreparation::Candidate {
                    request: PublishGenerationRequest {
                        authority_id: volume_authority_id(target.id),
                        volume_id: target.id,
                        epoch: expected_head.epoch,
                        expected: expected_head,
                        operation_id,
                        generation_root: candidate,
                    },
                    intent: PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
                    limits: closure_limits(target.config),
                    resolutions_digest: candidate.digest,
                    source_hold: None,
                })
            }
            .await;
            source_root.retire(accounting)?;
            result
        }
        .await;
        target_root.retire(accounting)?;
        base_root.retire(accounting)?;
        if let Ok(crate::workspace::JoinPreparation::Complete(output)) = &result {
            scope.keep_bytes(output.bytes)?;
        }
        result
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Checkout<A, O> {
    /// Reclassifies exact dependencies under the supplied work limits.
    pub async fn rebase_head<W: FsWork>(
        &mut self,
        maximum_conflicts: u32,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<RebaseDecision> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            accounting.check()?;
            if self.live_operation_id.is_some() {
                return Err(FsError::PendingLiveMutation);
            }
            self.rebase_head_with_accounting(maximum_conflicts, &mut accounting, cancellation)
                .await
        }
        .await;
        budget.finish_owned(result, &accounting)
    }

    pub(super) async fn rebase_head_with_accounting(
        &mut self,
        maximum_conflicts: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            RebaseDecision,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        FsError,
    > {
        accounting.check()?;
        if self.prepared_merge_parent.is_some() {
            return Err(FsError::PreparedMergePending);
        }
        if !matches!(
            self.mode.consistency,
            ConsistencyMode::TrackingSafe | ConsistencyMode::Manual | ConsistencyMode::Live
        ) {
            return Err(FsError::RefreshNotAllowed);
        }
        let (candidate_object, candidate_head) = self
            .volume
            .resolve_head_generation(accounting, cancellation)
            .await?;
        // The captured proof snapshot shares its map and owned names.
        let dependencies = self.dependencies.proof().dependencies.clone();
        if maximum_conflicts == 0 && candidate_object != self.root.base {
            return Err(FsError::Rebase(RebaseError::ZeroConflictLimit));
        }
        let certificate = if candidate_object == self.root.base {
            None
        } else {
            self.head_changes_disjoint(
                candidate_object,
                candidate_head,
                &dependencies,
                accounting,
                cancellation,
            )
            .await?
        };
        let disjoint = certificate.is_some();
        let (decision, allocation, settled) = if disjoint {
            (
                RebaseDecision::Safe {
                    generation: GenerationId::from_root(candidate_object),
                },
                accounting.claim_owned(0, 0)?,
                None,
            )
        } else {
            self.probe_rebase_head(
                candidate_object,
                &dependencies,
                maximum_conflicts,
                accounting,
                cancellation,
            )
            .await?
        };
        let classified = (decision, allocation);
        if matches!(classified.0, RebaseDecision::Conflicted { .. }) {
            return Ok(classified);
        }
        // The classifier settles only against a candidate that moved.
        if candidate_object == self.root.base {
            cancellation.check()?;
            self.authority_head = Some(candidate_head);
            self.observed_generation.refresh(
                GenerationId::from_root(candidate_object),
                candidate_head.sequence,
            );
            return Ok(classified);
        }
        self.prepare_rebase_adoption(
            ClassifiedRebase {
                object: candidate_object,
                head: candidate_head,
                decision: classified,
                settled,
                certificate,
            },
            maximum_conflicts,
            accounting,
            cancellation,
        )
        .await
    }

    async fn prepare_rebase_adoption(
        &mut self,
        classified: ClassifiedRebase<'_>,
        maximum_conflicts: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Decided, FsError> {
        let ClassifiedRebase {
            object,
            head,
            decision: classified,
            settled,
            certificate,
        } = classified;
        let outcome = self
            .prepare_rebased_head(
                object,
                maximum_conflicts,
                certificate.as_ref(),
                accounting,
                cancellation,
            )
            .await;
        let certificate = match &outcome {
            Ok(Some(_)) => certificate,
            _ => {
                if let Some(certificate) = certificate {
                    certificate.retire(accounting)?;
                }
                None
            }
        };
        let merged = match (outcome, settled) {
            (Ok(Some(merged)), settled) => (merged, settled),
            // A merge that did not merge what a replacement settled reports
            // those regions; without a settlement it breaks classification.
            (Ok(None), Some(settled)) => {
                crate::kernel::retire_allocation(classified.1, 0, accounting)?;
                return settled.into_conflicts(accounting);
            }
            (Ok(None), None) => return Err(FsError::InvalidDiff),
            (Err(error), settled) => {
                if let Some(settled) = settled {
                    settled.retire(accounting)?;
                }
                return Err(error);
            }
        };
        let ((root, prepared), settled) = merged;
        self.adopt_rebased_head(
            RebaseAdoption {
                object,
                head,
                classified,
                root,
                prepared,
                settled,
                certificate,
            },
            accounting,
            cancellation,
        )
    }

    pub(super) fn adopt_rebased_head(
        &mut self,
        adoption: RebaseAdoption<'_>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Decided, FsError> {
        let RebaseAdoption {
            object,
            head,
            classified,
            root: rebased,
            prepared,
            settled,
            certificate,
        } = adoption;
        let settled = match cancellation.check() {
            Ok(()) => settled,
            Err(error) => {
                if let Some(certificate) = certificate {
                    certificate.retire(accounting)?;
                }
                if let Some(settled) = settled {
                    settled.retire(accounting)?;
                }
                rebased.retire(accounting)?;
                return Err(error.into());
            }
        };
        // The followed ledger is built from the ledger as it is now, under
        // its lock, so nothing recorded since classification is lost.
        let ledger = Arc::clone(&self.dependencies.proof);
        let mut proof = ledger.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(certificate) = certificate {
            let preserved = certificate.preserves(&proof.dependencies, accounting);
            certificate.retire(accounting)?;
            if !preserved? {
                rebased.retire(accounting)?;
                crate::kernel::retire_allocation(classified.1, 0, accounting)?;
                return Err(FsError::Rebase(RebaseError::DependenciesChanged));
            }
        }
        let followed = match followed_ledger(
            &proof.dependencies,
            &self.dependencies.data_custody,
            settled,
            accounting,
        ) {
            Ok(Ok(followed)) => followed,
            Ok(Err(conflicted)) => {
                rebased.retire(accounting)?;
                crate::kernel::retire_allocation(classified.1, 0, accounting)?;
                return Ok(conflicted);
            }
            Err(error) => {
                rebased.retire(accounting)?;
                return Err(error);
            }
        };
        // Working-root transfer is the final fallible step. No operation
        self.install_root_replacement(object, rebased.value, prepared)?;
        if let Some(followed) = followed {
            proof.dependencies = followed;
        }
        drop(proof);
        self.generation_root = object;
        self.authority_head = Some(head);
        self.observed_generation
            .refresh(GenerationId::from_root(object), head.sequence);
        Ok(classified)
    }

    pub(super) async fn prepare_rebased_head(
        &self,
        candidate_object: ObjectId,
        maximum_conflicts: u32,
        certificate: Option<&super::rebase_footprint::DisjointChanges>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        Option<(
            Owned<GenerationRoot>,
            super::candidates::PreparedCheckoutRoots,
        )>,
        FsError,
    > {
        let mut frame = crate::heap_future::AdmittedFrame::new(accounting);
        let (future, _) = crate::heap_future::in_heap_admitted(
            &mut frame,
            |frame, bytes| frame.admit(bytes),
            |frame| async move {
                self.prepare_rebased_head_inner(
                    candidate_object,
                    maximum_conflicts,
                    certificate,
                    frame.accounting(),
                    cancellation,
                )
                .await
            },
        )?;
        let mut future = future.erase_task();
        let result = future.as_mut().await;
        drop(future);
        frame.finish(result)
    }

    async fn prepare_rebased_head_inner(
        &self,
        candidate_object: ObjectId,
        maximum_conflicts: u32,
        certificate: Option<&super::rebase_footprint::DisjointChanges>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        Option<(
            Owned<GenerationRoot>,
            super::candidates::PreparedCheckoutRoots,
        )>,
        FsError,
    > {
        let mut loaded = None;
        let candidate = if let Some(certificate) = certificate {
            &certificate.root.value
        } else {
            let (root, allocation) = crate::kernel::read_generation_root_with_accounting(
                &self.volume.fs.inner.objects,
                candidate_object,
                self.volume.config,
                accounting,
                cancellation,
            )
            .await?;
            let bytes = root_parent_bytes(&root)?;
            &loaded.insert(Owned::new(root, allocation, bytes)).value
        };
        let outcome = async {
            if candidate.volume_id != self.volume.id {
                return Err(FsError::VolumeMismatch);
            }
            let rebased = if let Some(certificate) = certificate {
                Some(
                    certificate
                        .compose(
                            &self.volume.fs.inner.objects,
                            candidate_object,
                            DecodeLimits::for_volume(self.volume.config),
                            accounting,
                            cancellation,
                        )
                        .await?,
                )
            } else {
                self.rebase_private_candidate_with_accounting(
                    candidate_object,
                    candidate,
                    maximum_conflicts,
                    accounting,
                    cancellation,
                )
                .await?
            };
            let Some((root, allocation)) = rebased else {
                return Ok(None);
            };
            let bytes = root_parent_bytes(&root)?;
            let rebased = Owned::new(root, allocation, bytes);
            #[cfg(not(target_arch = "wasm32"))]
            let mut rebased = rebased;
            let staged = self.prepare_root_replacement(
                candidate,
                &rebased.value,
                accounting,
                cancellation,
                None,
            );
            let prepared = match staged {
                Ok(prepared) => prepared,
                Err(error) => {
                    rebased.retire(accounting)?;
                    return Err(error);
                }
            };
            #[cfg(not(target_arch = "wasm32"))]
            let mut prepared = prepared;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(guard) =
                    &mut rebased.allocation
                else {
                    unreachable!("native root has original custody");
                };
                if let Err(error) = self.prepare_root_working_control_with_native_work(
                    &rebased.value,
                    guard,
                    &mut prepared,
                    original,
                    cancellation,
                ) {
                    rebased.retire(accounting)?;
                    return Err(error);
                }
            }
            Ok::<_, FsError>(Some((rebased, prepared)))
        }
        .await;
        if let Some(loaded) = loaded {
            loaded.retire(accounting).and(outcome)
        } else {
            outcome
        }
    }

    pub(super) async fn probe_rebase_head<'d>(
        &self,
        candidate_object: ObjectId,
        dependencies: &'d CheckoutDependencies,
        maximum_conflicts: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            RebaseDecision,
            KernelOwnedAllocation,
            Option<crate::kernel::Settled<'d>>,
        ),
        FsError,
    > {
        let mut frame = crate::heap_future::AdmittedFrame::new(accounting);
        let (future, _) = crate::heap_future::in_heap_admitted(
            &mut frame,
            |frame, bytes| frame.admit(bytes),
            |frame| async move {
                self.probe_rebase_head_inner(
                    candidate_object,
                    dependencies,
                    maximum_conflicts,
                    frame.accounting(),
                    cancellation,
                )
                .await
            },
        )?;
        let mut future = future.erase_task();
        let result = future.as_mut().await;
        drop(future);
        frame.finish(result)
    }

    async fn probe_rebase_head_inner<'d>(
        &self,
        candidate_object: ObjectId,
        dependencies: &'d CheckoutDependencies,
        maximum_conflicts: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            RebaseDecision,
            KernelOwnedAllocation,
            Option<crate::kernel::Settled<'d>>,
        ),
        FsError,
    > {
        let (probe, probe_allocation) = AuthenticatedGenerationProbe::new_with_accounting(
            &self.volume.fs.inner.objects,
            probe_limits(self.volume.config),
            accounting,
        )?;
        let probe = (probe, probe_allocation);
        let working = crate::kernel::WorkingRebaseProbe {
            probe: &probe.0,
            working: self.root.file_table,
        };
        let classified = crate::kernel::classify_rebase_settling_with_accounting(
            &working,
            GenerationId::from_root(self.root.base),
            GenerationId::from_root(candidate_object),
            dependencies,
            maximum_conflicts,
            accounting,
            cancellation,
        )
        .await
        .map_err(|error| match error {
            crate::kernel::ClassifyRebaseFailure::Probe(error)
            | crate::kernel::ClassifyRebaseFailure::Accounting(error) => error,
            crate::kernel::ClassifyRebaseFailure::ZeroConflictLimit => {
                FsError::Rebase(RebaseError::ZeroConflictLimit)
            }
        });
        let retired = {
            let (probe, _allocation) = probe;
            probe.retire_with_accounting(accounting)
        };
        match (retired, classified) {
            (Ok(()), classified) => classified,
            (Err(error), Ok((_, _, Some(settled)))) => {
                settled.retire(accounting)?;
                Err(error)
            }
            (Err(error), _) => Err(error),
        }
    }

    pub(super) async fn rebase_private_candidate_with_accounting(
        &self,
        candidate_object: ObjectId,
        candidate_root: &GenerationRoot,
        maximum_conflicts: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        Option<(
            GenerationRoot,
            crate::kernel::allocation::KernelOwnedAllocation,
        )>,
        FsError,
    > {
        use crate::kernel::{clone_generation_root_with_accounting, retire_allocation};
        if self.root.file_table == self.base_root.file_table {
            return clone_generation_root_with_accounting(candidate_root, accounting).map(Some);
        }
        let checkpoint = crate::kernel::build_checkpoint_with_accounting(
            &self.volume.fs.inner.objects,
            CheckpointRequest {
                base: self.root.base,
                file_table: self.root.file_table,
                merge_parent: None,
            },
            DecodeLimits::for_volume(self.volume.config),
            accounting,
            cancellation,
        )
        .await?;
        let base = clone_generation_root_with_accounting(&self.base_root, accounting)?;
        let ours = match clone_generation_root_with_accounting(candidate_root, accounting) {
            Ok(ours) => ours,
            Err(error) => {
                let bytes = root_parent_bytes(&base.0)?;
                drop(base.0);
                retire_allocation(base.1, bytes, accounting)?;
                return Err(error);
            }
        };
        let theirs = match clone_generation_root_with_accounting(&self.root, accounting) {
            Ok(theirs) => theirs,
            Err(error) => {
                let base_bytes = root_parent_bytes(&base.0)?;
                let ours_bytes = root_parent_bytes(&ours.0)?;
                drop(base.0);
                drop(ours.0);
                retire_allocation(base.1, base_bytes, accounting)?;
                retire_allocation(ours.1, ours_bytes, accounting)?;
                return Err(error);
            }
        };
        let input_bytes = [
            root_parent_bytes(&base.0)?,
            root_parent_bytes(&ours.0)?,
            root_parent_bytes(&theirs.0)?,
        ];
        let merged = crate::kernel::merge_generation_with_accounting(
            &self.volume.fs.inner.objects,
            MergeGenerationRequest {
                base_generation: self.root.base,
                base: base.0,
                ours_generation: Some(candidate_object),
                ours: ours.0,
                theirs_generation: checkpoint,
                theirs: theirs.0,
                retain_theirs_parent: false,
                maximum_changes: u32::MAX,
                maximum_conflicts,
                resolutions: BTreeMap::new(),
            },
            // The checkout's dependencies are byte regions; so is its merge.
            crate::kernel::ContentMerge::Bytes,
            DecodeLimits::for_volume(self.volume.config),
            accounting,
            cancellation,
        )
        .await;
        retire_allocation(base.1, input_bytes[0], accounting)?;
        retire_allocation(ours.1, input_bytes[1], accounting)?;
        retire_allocation(theirs.1, input_bytes[2], accounting)?;
        let (outcome, allocation) = merged?;
        match outcome {
            MergeGenerationOutcome::Prepared { root, .. } => Ok(Some((root, allocation))),
            MergeGenerationOutcome::Conflicted { conflicts, .. } => {
                let bytes = conflicts.iter().try_fold(
                    crate::foundation::usize_to_u64(
                        conflicts.capacity() * size_of::<crate::kernel::MergeConflict>(),
                    ),
                    |bytes, conflict| {
                        let nested = match conflict {
                            crate::kernel::MergeConflict::Binding { name, .. } => {
                                name.retained_bytes()
                            }
                            crate::kernel::MergeConflict::File(_) => 0,
                        };
                        bytes
                            .checked_add(crate::foundation::usize_to_u64(nested))
                            .ok_or(WorkError::Overflow)
                    },
                )?;
                drop(conflicts);
                retire_allocation(allocation, bytes, accounting)?;
                Ok(None)
            }
        }
    }
}

pub(super) fn probe_limits(config: VolumeConfig) -> ProbeLimits {
    ProbeLimits {
        decode: DecodeLimits::for_volume(config),
        maximum_cached_generations: 2,
        maximum_cached_records: config.limits.maximum_checkout_dependencies,
        maximum_extent_spans: config.limits.maximum_directory_page_entries,
        maximum_content_payload_bytes: config.limits.maximum_read_bytes,
        maximum_directory_entries: config.limits.maximum_directory_page_entries,
    }
}

type Decided = (
    RebaseDecision,
    crate::kernel::allocation::KernelOwnedAllocation,
);

/// The ledger a rebase installs: `dependencies` with each settlement
/// followed, built and charged to `owner` before the rebased root is
/// installed. Two settlements that now expect different states of one region
/// make the rebase conflict on its settled regions instead.
fn followed_ledger(
    dependencies: &CheckoutDependencies,
    owner: &Arc<crate::kernel::ProofDataCustody>,
    settled: Option<crate::kernel::Settled<'_>>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<Result<Option<CheckoutDependencies>, Decided>, FsError> {
    let Some(mut settled) = settled else {
        return Ok(Ok(None));
    };
    match settled.follow(dependencies, owner, accounting) {
        Ok(Some(followed)) => settled.retire(accounting).map(|()| Ok(Some(followed))),
        Ok(None) => settled.into_conflicts(accounting).map(Err),
        Err(error) => {
            settled.retire(accounting)?;
            Err(error)
        }
    }
}
