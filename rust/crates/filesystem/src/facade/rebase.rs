use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    #[allow(
        clippy::too_many_lines,
        reason = "one live rebase replays every candidate change against the advanced head under a single fence"
    )]
    pub(crate) async fn live_rebase_workspace(
        &self,
        request: WorkspaceRebaseRequest<'_, A, O>,
    ) -> Result<WorkspaceJoinOutcome, WorkspaceError> {
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
        let cancellation = CancellationToken::new();
        let RebaseLineage {
            target_object,
            target_head: expected_head,
            target_root,
            base_object,
            base_root,
            source,
            source_object,
        } = self
            .rebase_lineage(target, maximum_generations, &cancellation)
            .await?;
        if source_object == base_object {
            return Ok(WorkspaceJoinOutcome::NoChanges(GenerationId::new(
                target_object.digest,
            )));
        }
        let (source_root, _) = read_generation_root(
            &self.inner.objects,
            source_object,
            target.config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        if source_root.volume_id != source.id
            || source_root.root_file_id != target_root.root_file_id
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }

        let normalize = |root: &GenerationRoot| GenerationRoot {
            volume_id: target.id,
            root_file_id: target_root.root_file_id,
            file_table: root.file_table,
            parents: Vec::new(),
            required_features: root.continuation_features(),
        };
        let normalized_base = normalize(&base_root);
        let normalized_source = normalize(&source_root);
        drop(base_root);
        drop(source_root);
        let retained_root_bytes =
            root_parent_bytes(&target_root).map_err(WorkspaceError::engine)?;
        let (normalized_base_object, _) = self
            .put_generation_root(
                &normalized_base,
                WorkCounters::default(),
                WorkBudget::UNBOUNDED,
                &cancellation,
                retained_root_bytes,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        let (normalized_source_object, _) = self
            .put_generation_root(
                &normalized_source,
                WorkCounters::default(),
                WorkBudget::UNBOUNDED,
                &cancellation,
                retained_root_bytes,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        let merged = merge_generation_async(
            &self.inner.objects,
            MergeGenerationRequest {
                base_generation: normalized_base_object,
                base: normalized_base,
                ours_generation: Some(normalized_source_object),
                ours: normalized_source,
                theirs_generation: target_object,
                theirs: target_root,
                retain_theirs_parent: false,
                maximum_changes,
                maximum_conflicts,
                resolutions: BTreeMap::new(),
            },
            DecodeLimits::for_volume(target.config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        let merged_root = match merged.value {
            MergeGenerationOutcome::Conflicted {
                conflicts,
                truncated,
            } => return Ok(WorkspaceJoinOutcome::Conflicted(conflicts, truncated)),
            MergeGenerationOutcome::Prepared { root, .. } => root,
        };
        let parent_buffer = root_parent_buffer(
            &[GenerationId::new(source_object.digest)],
            root_parent_bytes(&merged_root).map_err(WorkspaceError::engine)?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .map_err(WorkspaceError::engine)?;
        let rebased_root = GenerationRoot {
            volume_id: target.id,
            root_file_id: merged_root.root_file_id,
            file_table: merged_root.file_table,
            parents: parent_buffer.value,
            required_features: merged_root.continuation_features(),
        };
        drop(merged_root);
        let (candidate, _) = self
            .put_generation_root(
                &rebased_root,
                parent_buffer.work,
                WorkBudget::UNBOUNDED,
                &cancellation,
                root_parent_bytes(&rebased_root).map_err(WorkspaceError::engine)?,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        let publication = publish_generation(
            &self.inner.objects,
            &self.inner.authority,
            PublishGenerationRequest {
                authority_id: volume_authority_id(target.id),
                volume_id: target.id,
                epoch: expected_head.epoch,
                expected: expected_head,
                operation_id,
                generation_root: candidate,
            },
            closure_limits(target.config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        Ok(match publication.outcome {
            AppendOutcome::Committed(_) => {
                WorkspaceJoinOutcome::Applied(publication.proof.generation_id)
            }
            AppendOutcome::AlreadyCommitted(_) => {
                WorkspaceJoinOutcome::AlreadyApplied(publication.proof.generation_id)
            }
            AppendOutcome::Conflict { .. } => {
                let (actual, _, _) = target
                    .resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(WorkspaceError::engine)?;
                WorkspaceJoinOutcome::Stale(GenerationId::new(actual.digest))
            }
            AppendOutcome::Fenced { .. } => WorkspaceJoinOutcome::Fenced,
            AppendOutcome::IdempotencyConflict { .. } => WorkspaceJoinOutcome::IdempotencyConflict,
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
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
        use crate::kernel::Owned;
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
        let (candidate_object, candidate_head) = {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.volume
                    .resolve_head_generation(original, cancellation)
                    .await
                    .map(|(root, head, _)| (root, head))?
            } else {
                self.resolve_rebase_head_standalone(accounting, cancellation)
                    .await?
            }
            #[cfg(target_arch = "wasm32")]
            self.resolve_rebase_head_standalone(accounting, cancellation)
                .await?
        };
        let (probe, probe_allocation) = AuthenticatedGenerationProbe::new_with_accounting(
            &self.volume.fs.inner.objects,
            probe_limits(self.volume.config),
            accounting,
        )?;
        let probe = (probe, probe_allocation);
        // The proof snapshot is an Arc share, not a copied map or name graph.
        let dependencies = self.dependencies.proof().dependencies.clone();
        let classified = crate::kernel::classify_rebase_with_accounting(
            &probe.0,
            GenerationId::new(self.root.base().digest),
            GenerationId::new(candidate_object.digest),
            &dependencies,
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
        let retired = probe.0.retire_with_accounting(accounting);
        drop(probe.1);
        retired?;
        let classified = classified?;
        if matches!(classified.0, RebaseDecision::Conflicted { .. }) {
            return Ok(classified);
        }
        if candidate_object == self.root.base() {
            cancellation.check()?;
            self.authority_head = Some(candidate_head);
            return Ok(classified);
        }
        let (candidate_root, allocation) = crate::kernel::read_generation_root_with_accounting(
            &self.volume.fs.inner.objects,
            candidate_object,
            self.volume.config,
            accounting,
            cancellation,
        )
        .await?;
        let candidate_bytes = root_parent_bytes(&candidate_root)?;
        let candidate = Owned::new(candidate_root, allocation, candidate_bytes);
        let outcome = async {
            if candidate.value.volume_id != self.volume.id {
                return Err(FsError::VolumeMismatch);
            }
            let candidate_file_table = candidate.value.file_table;
            let (root, allocation) = self
                .rebase_private_candidate_with_accounting(
                    candidate_object,
                    &candidate.value,
                    maximum_conflicts,
                    accounting,
                    cancellation,
                )
                .await?;
            let bytes = root_parent_bytes(&root)?;
            let rebased = Owned::new(root, allocation, bytes);
            let staged = self.prepare_root_replacement(
                &candidate.value,
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
            Ok::<_, FsError>((rebased, prepared, candidate_file_table))
        }
        .await;
        candidate.retire(accounting)?;
        let (mut rebased, mut prepared, candidate_file_table) = outcome?;
        if let Err(error) = cancellation.check() {
            rebased.retire(accounting)?;
            return Err(error.into());
        }
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
        // Working-root transfer is the final fallible step. No operation
        self.install_root_replacement(candidate_object, rebased.value, prepared);
        self.base_file_table = candidate_file_table;
        self.generation_root = candidate_object;
        self.authority_head = Some(candidate_head);
        drop(rebased.allocation);
        Ok(classified)
    }

    pub(super) async fn resolve_rebase_head_standalone(
        &self,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Head), FsError> {
        match self
            .volume
            .resolve_head_generation(accounting.remaining()?, cancellation)
            .await
            .map(|receipt| receipt.value)
        {
            Ok((root, head, work)) => {
                accounting.record_completed(work)?;
                Ok((root, head))
            }
            Err(failure) => {
                accounting.record_completed(*failure.work)?;
                Err(failure.error)
            }
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
        (
            GenerationRoot,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        FsError,
    > {
        use crate::kernel::{clone_generation_root_with_accounting, retire_allocation};
        if self.root.file_table == self.base_file_table {
            return clone_generation_root_with_accounting(candidate_root, accounting);
        }
        let checkpoint = crate::kernel::build_checkpoint_with_accounting(
            &self.volume.fs.inner.objects,
            CheckpointRequest {
                base: self.root.base(),
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
                base_generation: self.root.base(),
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
            MergeGenerationOutcome::Prepared { root, .. } => Ok((root, allocation)),
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
                Err(FsError::InvalidDiff)
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
