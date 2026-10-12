use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn apply_workspace_join(
        &self,
        request: WorkspaceJoinRequest<'_, A, O>,
    ) -> Result<WorkspaceJoinOutcome, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let output = self
            .apply_workspace_join_with_accounting(request, &mut accounting, &cancellation, &mut ())
            .await?;
        Ok(output.value)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn apply_workspace_join_guarded_with_native_work<'original, F, Fut, E>(
        &self,
        request: WorkspaceJoinRequest<'_, A, O>,
        original: &'original NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<(WorkspaceJoinOutcome, NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        let mut accounting = KernelAccounting::native(original, cancellation);
        let output = self
            .apply_workspace_join_with_accounting(
                request,
                &mut accounting,
                cancellation,
                &mut (original, before_publication),
            )
            .await?;
        match output.allocation {
            KernelOwnedAllocation::NativeOriginal(allocation) => Ok((output.value, allocation)),
            KernelOwnedAllocation::Standalone => {
                unreachable!("native join retains original output custody")
            }
        }
    }

    pub(super) async fn apply_workspace_join_with_accounting<P>(
        &self,
        request: WorkspaceJoinRequest<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
        publication: &mut P,
    ) -> Result<Owned<WorkspaceJoinOutcome>, P::Error>
    where
        P: WorkspaceJoinPublication<A, O>,
    {
        use Owned;
        accounting.check()?;
        let resolutions_digest = hash_join_resolutions(&request.resolutions, accounting)?;
        let hashing = WorkCounters {
            bytes_hashed: WORKSPACE_JOIN_OPERATION_DOMAIN.len() as u64 + 189,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(hashing)?;
        let operation_context = request.operation_context(resolutions_digest);
        accounting.record_completed(hashing)?;
        let WorkspaceJoinRequest {
            target,
            base,
            source,
            expected_target,
            expected_head,
            history,
            operation_id,
            permit,
            maximum_generations,
            maximum_changes,
            maximum_conflicts,
            resolutions,
        } = request;
        if maximum_generations == 0 || maximum_changes == 0 || maximum_conflicts == 0 {
            return Err(WorkspaceError::JoinLimit.into());
        }
        if !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            || source.workspace.volume.config != target.config
        {
            return Err(WorkspaceError::IncompatibleWorkspace.into());
        }
        let current_target = self
            .workspace_head_state(target, accounting, cancellation)
            .await?
            .0;
        let target_object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: expected_target.digest(),
        };
        let base_object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: base.id.digest(),
        };
        let source_object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: source.id.digest(),
        };
        let mut base_root = self
            .read_join_root_with_accounting(base_object, target.config, accounting, cancellation)
            .await?;
        let mut source_root = self
            .read_join_root_with_accounting(source_object, target.config, accounting, cancellation)
            .await?;
        let target_root = self
            .read_join_root_with_accounting(target_object, target.config, accounting, cancellation)
            .await?;
        let result = async {
            if source_root.value.volume_id != source.workspace.volume.id
                || target_root.value.volume_id != target.id
                || base_root.value.root_file_id != target_root.value.root_file_id
                || source_root.value.root_file_id != target_root.value.root_file_id
            {
                return Err(WorkspaceError::IncompatibleWorkspace.into());
            }
            // Normalize scalar identity in place; each decoded parent vector keeps
            base_root.value.required_features = base_root.value.continuation_features();
            base_root.value.volume_id = target.id;
            source_root.value.required_features = source_root.value.continuation_features();
            source_root.value.volume_id = target.id;
            let encoded =
                crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                    crate::kernel::encode_generation_root(
                        &base_root.value,
                        observe,
                        allocation_error,
                    )
                })?;
            let normalized_base_object = self
                .put_encoded_with_accounting(
                    ObjectKind::GenerationRoot,
                    encoded,
                    accounting,
                    cancellation,
                )
                .await?;
            let encoded =
                crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                    crate::kernel::encode_generation_root(
                        &source_root.value,
                        observe,
                        allocation_error,
                    )
                })?;
            let normalized_source_object = self
                .put_encoded_with_accounting(
                    ObjectKind::GenerationRoot,
                    encoded,
                    accounting,
                    cancellation,
                )
                .await?;
            if normalized_source_object == target_object && current_target == expected_target {
                return workspace_join_scalar(
                    WorkspaceJoinOutcome::NoChanges(current_target),
                    accounting,
                )
                .map_err(P::Error::from);
            }
            let merge_request = crate::kernel::MergeGenerationBorrowedRequest {
                base_generation: normalized_base_object,
                base: &base_root.value,
                ours_generation: Some(target_object),
                ours: &target_root.value,
                theirs_generation: normalized_source_object,
                theirs: &source_root.value,
                retain_theirs_parent: history == crate::workspace::JoinHistory::Merge,
                maximum_changes,
                maximum_conflicts,
            };
            let (merged, allocation) = match &resolutions {
                WorkspaceJoinResolutions::Map(map) => {
                    merge_generation_with_borrowed_map_resolutions_with_accounting(
                        &self.inner.objects,
                        &merge_request,
                        map,
                        DecodeLimits::for_volume(target.config),
                        accounting,
                        cancellation,
                    )
                    .await?
                }
                WorkspaceJoinResolutions::Slice(slice) => {
                    merge_generation_with_borrowed_resolutions_with_accounting(
                        &self.inner.objects,
                        merge_request,
                        slice,
                        DecodeLimits::for_volume(target.config),
                        accounting,
                        cancellation,
                    )
                    .await?
                }
            };
            let (candidate, root) = match merged {
                MergeGenerationOutcome::Conflicted {
                    conflicts,
                    truncated,
                } => {
                    let mut conflicts = Owned::new(
                        WorkspaceJoinOutcome::Conflicted(conflicts, truncated),
                        allocation,
                        0,
                    );
                    if let WorkspaceJoinOutcome::Conflicted(values, _) = &conflicts.value {
                        conflicts.bytes = workspace_join_conflict_bytes(values)?;
                    }
                    return Ok(conflicts);
                }
                MergeGenerationOutcome::Prepared {
                    generation_root,
                    root,
                    ..
                } => {
                    let mut root = Owned::new(root, allocation, 0);
                    root.bytes = root_parent_bytes(&root.value)?;
                    (generation_root, root)
                }
            };
            let unchanged = root.value.file_table == target_root.value.file_table;
            root.retire(accounting)?;
            if unchanged {
                return workspace_join_scalar(
                    WorkspaceJoinOutcome::NoChanges(current_target),
                    accounting,
                )
                .map_err(P::Error::from);
            }
            // This immutable normalized history parent is outside the candidate's
            let _source_hold = if history == crate::workspace::JoinHistory::Merge {
                Some(
                    self.flush_join_parent_with_accounting(
                        normalized_source_object,
                        accounting,
                        cancellation,
                    )
                    .await?,
                )
            } else {
                None
            };
            let published = publication
                .publish(
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
                    PublicationIntent::guarded(Some(operation_context), permit),
                    closure_limits(target.config),
                    accounting,
                    cancellation,
                )
                .await?;
            let outcome = match published {
                LivePublicationObservation::Committed {
                    generation_id: id, ..
                } => WorkspaceJoinOutcome::Joined(id, resolutions_digest),
                LivePublicationObservation::AlreadyCommitted {
                    generation_id: id, ..
                } => WorkspaceJoinOutcome::AlreadyJoined(id, resolutions_digest),
                LivePublicationObservation::Conflict { .. } => WorkspaceJoinOutcome::Stale(
                    self.workspace_head_state(target, accounting, cancellation)
                        .await?
                        .0,
                ),
                LivePublicationObservation::Fenced { .. } => WorkspaceJoinOutcome::Fenced,
                LivePublicationObservation::IdempotencyConflict { .. } => {
                    WorkspaceJoinOutcome::IdempotencyConflict
                }
            };
            workspace_join_scalar(outcome, accounting).map_err(P::Error::from)
        }
        .await;
        target_root.retire(accounting)?;
        source_root.retire(accounting)?;
        base_root.retire(accounting)?;
        result
    }

    pub(super) async fn read_join_root_with_accounting(
        &self,
        object: ObjectId,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<GenerationRoot>, FsError> {
        let (root, allocation) = read_generation_root_with_accounting(
            &self.inner.objects,
            object,
            config,
            accounting,
            cancellation,
        )
        .await?;
        let mut root = Owned::new(root, allocation, 0);
        root.bytes = root_parent_bytes(&root.value)?;
        Ok(root)
    }
    pub(super) async fn flush_join_parent_with_accounting(
        &self,
        parent: ObjectId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, FsError> {
        self.flush_publication(
            crate::PublicationScope::Closure {
                objects: &[parent],
                proven_at: self.inner.objects.collection_sweeps(),
            },
            accounting,
            cancellation,
        )
        .await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Prepares one bounded two-parent merge against this checkout's immutable base.
    pub async fn prepare_merge(
        &mut self,
        theirs: GenerationId,
        maximum_changes: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<MergePreparation> {
        if self.mode.access != AccessMode::ReadWrite
            || self.mode.mutations != MutationMode::PrivateOverlay
        {
            return Err(OperationFailure::before_work(FsError::MutationNotAllowed));
        }
        if self.prepared_merge_parent.is_some() {
            return Err(OperationFailure::before_work(FsError::PreparedMergePending));
        }
        if maximum_changes == 0 || maximum_conflicts == 0 {
            return Err(OperationFailure::before_work(FsError::MergeChangeLimit));
        }
        let theirs_id = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: theirs.digest(),
        };
        let (current_head_root, current_head, mut work) = self
            .volume
            .resolve_head_generation(budget, cancellation)
            .await
            .map(|receipt| receipt.value)?;
        if current_head_root != theirs_id {
            return Err(OperationFailure::new(FsError::MergeParentNotHead, work));
        }
        let (theirs_root, theirs_work) = read_generation_root(
            &self.volume.fs.inner.objects,
            theirs_id,
            self.volume.config,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, theirs_work)?;
        let merged = merge_generation_async(
            &self.volume.fs.inner.objects,
            MergeGenerationRequest {
                base_generation: self.root.base(),
                base: self.base_root.clone(),
                ours_generation: (self.root.file_table == self.base_file_table)
                    .then_some(self.generation_root),
                ours: self.root.clone(),
                theirs_generation: theirs_id,
                theirs: theirs_root,
                retain_theirs_parent: true,
                maximum_changes,
                maximum_conflicts,
                resolutions: BTreeMap::new(),
            },
            DecodeLimits::for_volume(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| map_merge_failure(failure, work))?;
        work = add(work, merged.work)?;
        match merged.value {
            MergeGenerationOutcome::Prepared {
                generation_root,
                root,
                generation_id,
            } => {
                self.root.set_working(root);
                self.generation_root = generation_root;
                self.prepared_merge_parent = Some(theirs_id);
                self.authority_head = Some(current_head);
                Ok(OperationReceipt {
                    value: MergePreparation::Prepared { generation_id },
                    work,
                })
            }
            MergeGenerationOutcome::Conflicted {
                conflicts,
                truncated,
            } => Ok(OperationReceipt {
                value: MergePreparation::Conflicted {
                    conflicts,
                    truncated,
                },
                work,
            }),
        }
    }
}
