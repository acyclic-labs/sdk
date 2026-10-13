use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    ParentProjectController<'_, A, O>
{
    /// Forks the parent's exact project generation into a child project.
    pub async fn fork_project(
        &self,
        source_generation: &GenerationRef,
        child: &VolumeRef,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation> {
        self.require(capability::FORK_PUBLISH, VolumeOperation::Read, None)?;
        self.host
            .fork_volume_pinned(
                &self.project,
                source_generation,
                child,
                None,
                idempotency_key,
            )
            .await
    }

    /// Inspects an exact readable source, regardless of agent ancestry.
    pub async fn prepare_project_import(
        &self,
        source: &VolumeRef,
        generation: &GenerationRef,
        target: Option<&GenerationRef>,
    ) -> Result<ParentMergePlan<A, O>> {
        self.prepare_project_import_checked(source, generation, target, false, || {
            std::future::ready(Ok(()))
        })
        .await
    }

    pub(in crate::filesystem) fn require_selected_project_source(
        source: &VolumeRef,
        generation: &GenerationRef,
        handle: &crate::source_authorization::ProjectSourceReadHandle,
        context: &crate::runtime::TaskContext,
        operation: crate::OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let authorization = handle.authorization();
        if context.durable_task_id() != Some(authorization.caller_task) {
            #[cfg(not(target_arch = "wasm32"))]
            if io.is_some_and(|io| io.original_native_work().is_some()) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "selected project import belongs to another original caller",
                });
            }
            return Err(Error::Unauthorized(
                "selected project import belongs to another original caller".into(),
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            handle.require_selection(
                operation,
                authorization.source_task,
                source,
                generation,
                authorization.native_snapshot.as_ref(),
                Some(work),
            )?;
            return handle.grant().require(source, VolumeOperation::Read, io);
        }
        handle.require_selection(
            operation,
            authorization.source_task,
            source,
            generation,
            authorization.native_snapshot.as_ref(),
            None,
        )?;
        handle.grant().require(source, VolumeOperation::Read, io)
    }

    /// Plans the exact approved source at an optional original target cut.
    pub async fn prepare_project_import_from_selected_source<P: acyclic_stream::StreamProvider>(
        &self,
        source: &VolumeRef,
        generation: &GenerationRef,
        target: Option<&GenerationRef>,
        handle: &crate::source_authorization::ProjectSourceReadHandle,
        owner: &Arc<crate::durable_host::TaskJournalOwner<P>>,
        context: &crate::runtime::TaskContext,
        operation: crate::OperationId,
    ) -> Result<ParentMergePlan<A, O>> {
        #[cfg(not(target_arch = "wasm32"))]
        let io = self
            .native_custody
            .as_ref()
            .map(|custody| PreparationIo::from_native_work(custody.work()))
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io: Option<PreparationIo> = None;
        self.require(
            capability::PROJECT_MERGE,
            VolumeOperation::Write,
            io.as_ref(),
        )?;
        Self::require_selected_project_source(
            source,
            generation,
            handle,
            context,
            operation,
            io.as_ref(),
        )?;
        self.prepare_project_import_checked(source, generation, target, true, || async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = &io {
                return owner
                    .verify_project_source_handle(
                        context,
                        handle,
                        Some(
                            io.original_native_work()
                                .expect("native controller custody"),
                        ),
                    )
                    .await;
            }
            owner
                .verify_project_source_handle(context, handle, None)
                .await
        })
        .await
    }

    pub(crate) async fn prepare_project_import_checked<F, Fut>(
        &self,
        source: &VolumeRef,
        generation: &GenerationRef,
        selected_target: Option<&GenerationRef>,
        selected: bool,
        mut guard: F,
    ) -> Result<ParentMergePlan<A, O>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        #[cfg(not(target_arch = "wasm32"))]
        let native_io = self
            .native_custody
            .as_ref()
            .map(|custody| {
                let io = PreparationIo::from_native_work(custody.work())?;
                self.require(capability::PROJECT_MERGE, VolumeOperation::Write, Some(&io))?;
                source.validate(&mut Some(&mut io.history_io()))?;
                if source.provider() != &self.host.provider
                    || (!selected
                        && !io.contains_capability(
                            self.scope.capabilities(),
                            source,
                            "volume:read:",
                        )?)
                {
                    return Err(stock_boundary(
                        Some(&io),
                        crate::wire::ErrorCode::Unauthorized,
                        "project import source is not currently readable by this parent",
                    ));
                }
                Ok(io)
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let native_io: Option<PreparationIo> = None;
        let cancellation = acyclic_fs::CancellationToken::new();
        #[cfg(not(target_arch = "wasm32"))]
        let native = native_io.is_some();
        let io = match native_io {
            Some(io) => io,
            None => {
                self.require(capability::PROJECT_MERGE, VolumeOperation::Write, None)?;
                if !selected {
                    ContentGrant::verify(
                        &self.verifier,
                        &self.scope,
                        source,
                        VolumeOperation::Read,
                        u64::MAX,
                        &mut None,
                    )?;
                }
                source.validate(&mut None)?;
                self.host.validate_provider(source.provider())?;
                PreparationIo::for_unmeasured(&cancellation)?
            }
        };
        guard().await?;
        let source_ref =
            fork_preparer_io::content_workspace(&self.host.provider, source, Some(&io))?;
        let target_ref =
            fork_preparer_io::content_workspace(&self.host.provider, &self.project, Some(&io))?;
        let source_workspace = fork_preparer_io::open(self.host, &source_ref, &io).await?;
        guard().await?;
        let target_workspace = fork_preparer_io::open(self.host, &target_ref, &io).await?;
        guard().await?;
        let source_generation =
            fork_preparer_io::generation(self.host, &source_workspace, generation, &io).await?;
        guard().await?;
        let target_generation = if let Some(target) = selected_target {
            fork_preparer_io::generation(self.host, &target_workspace, target, &io).await?
        } else {
            io.remaining()?;
            io.sdk(target_workspace.head(&io, io.cancellation()).await)?
        };
        guard().await?;
        let join = source_workspace.join_into(&target_workspace);
        io.remaining()?;
        let plan = io.sdk(
            join.plan_at(
                &source_generation,
                &target_generation,
                &io,
                io.cancellation(),
            )
            .await,
        )?;
        io.remaining()?;
        guard().await?;
        let (child_project, parent_project, parent_scope) =
            crate::executor::with_history_io(Some(&io), |work| -> Result<_> {
                Ok((
                    source.copy(work)?,
                    self.project.copy(work)?,
                    self.scope.copy(work)?,
                ))
            })?;
        drop(source_ref);
        drop(target_ref);
        #[cfg(not(target_arch = "wasm32"))]
        let native_custody = if native {
            let scope_owned = parent_scope.logical_owned_bytes()?;
            let retained = child_project
                .logical_owned_bytes()?
                .checked_add(parent_project.logical_owned_bytes()?)
                .and_then(|bytes| bytes.checked_add(scope_owned))
                .ok_or_else(|| io.invalid("native merge plan ownership overflows"))?;
            Some(io.into_native_custody(retained)?)
        } else {
            None
        };
        Ok(ParentMergePlan {
            plan,
            child_project,
            parent_project,
            parent_scope,
            #[cfg(not(target_arch = "wasm32"))]
            native_custody,
        })
    }

    /// Publishes the inspected plan with optional exact conflict-side selections.
    pub async fn apply_project_merge(
        &self,
        plan: &ParentMergePlan<A, O>,
        operation_id: crate::OperationId,
        selections: BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<(JoinOutcome<A, O>, Option<MergeAllocation>)> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = &self.native_custody {
            let work = custody.work();
            let io = PreparationIo::from_native_work(work)?;
            self.require_merge_plan(plan, Some(&io))?;
            self.verifier
                .verify(&self.scope, &mut Some(&mut io.history_io()))?;
            let verifier = self.verifier.copy(&mut Some(&mut io.history_io()))?;
            let scope = self.scope.copy(&mut Some(&mut io.history_io()))?;
            let project = self.project.copy(&mut Some(&mut io.history_io()))?;
            let bytes = std::mem::size_of::<(
                AuthorityVerifier,
                Scope,
                VolumeRef,
                acyclic_fs::NativeAllocation,
            )>() as u64;
            io.retain_owned(bytes)?;
            io.allocation_attempt(bytes)?;
            let retained = io.owned_bytes()?;
            let owner = Arc::new((verifier, scope, project, io.into_native_custody(retained)?));
            let mut guard = move |original: &acyclic_fs::NativeMaterializationWork| {
                std::future::ready((|| {
                    if !owner.3.work().shares_allocation_context(original) {
                        return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unauthorized,
                            reason: "merge publication owner belongs to another original resource",
                        });
                    }
                    let io = PreparationIo::from_native_work(original)?;
                    owner.0.verify(&owner.1, &mut Some(&mut io.history_io()))?;
                    if !owner.1.capabilities().contains(capability::PROJECT_MERGE)
                        || !io.contains_capability(
                            owner.1.capabilities(),
                            &owner.2,
                            "volume:write:",
                        )?
                    {
                        return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unauthorized,
                            reason: "parent project publication is not currently granted",
                        });
                    }
                    Ok(())
                })())
            };
            let options = Self::merge_options(
                plan,
                project_join_key(operation_id, Some(&PreparationIo::from_native_work(work)?))?,
            );
            let (outcome, custody) = if selections.is_empty() {
                plan.plan
                    .apply_with_native_work(
                        options,
                        acyclic_fs::PublicationPermit::Unrestricted,
                        work,
                        work.cancellation(),
                        &mut guard,
                    )
                    .await?
            } else {
                plan.plan
                    .apply_sides_with_permit_guarded_with_native_work(
                        options,
                        &selections,
                        acyclic_fs::PublicationPermit::Unrestricted,
                        work,
                        work.cancellation(),
                        &mut guard,
                    )
                    .await?
            };
            return Ok((outcome, Some(custody)));
        }
        self.require_merge_plan(plan, None)?;
        let options = Self::merge_options(plan, project_join_key(operation_id, None)?);
        let outcome = if selections.is_empty() {
            plan.plan
                .apply(options, acyclic_fs::PublicationPermit::Unrestricted)
                .await
                .map_err(map_error)?
        } else {
            plan.plan
                .apply_sides(options, selections)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?
        };
        Ok((outcome, None))
    }

    /// Converts a durable successful join into its immutable receipt and native custody.
    pub fn merge_receipt(
        &self,
        plan: &ParentMergePlan<A, O>,
        outcome: &JoinOutcome<A, O>,
        child: &Authority,
        operation_id: crate::OperationId,
        notice: &crate::conversation::ConversationMessage,
    ) -> Result<(ProjectMergeReceipt, Option<MergeAllocation>)> {
        #[cfg(not(target_arch = "wasm32"))]
        let io = self
            .native_custody
            .as_ref()
            .map(|custody| PreparationIo::from_native_work(custody.work()))
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io: Option<PreparationIo> = None;
        self.require_merge_plan(plan, io.as_ref())?;
        let (JoinOutcome::Applied(application) | JoinOutcome::AlreadyApplied(application)) =
            outcome
        else {
            return Err(crate::executor::stock_boundary(
                io.as_ref(),
                crate::wire::ErrorCode::Conflict,
                "project join did not publish a result",
            ));
        };
        let filesystem_key = project_join_key(operation_id, io.as_ref())?;
        let witness = match &io {
            Some(io) => io.sdk(plan.plan.commit_witness(
                application,
                filesystem_key,
                io,
                io.cancellation(),
            ))?,
            None => plan
                .plan
                .commit_witness(
                    application,
                    filesystem_key,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .map(|receipt| receipt.value)
                .map_err(|failure| map_error(failure.error))?,
        };
        let statement = match &io {
            Some(io) => {
                let maximum_witness_bytes = (std::mem::size_of::<JoinCommitWitness>() as u64)
                    .checked_mul(16)
                    .and_then(|bytes| bytes.checked_add(1024))
                    .ok_or_else(|| io.invalid("join witness bound overflows"))?;
                let root = std::mem::size_of::<serde_json::Value>() as u64;
                io.retain_owned(root)?;
                let root_hold = io.guard_owned(root)?;
                let diagnostics = io.reserve_decode(crate::executor::json::sdk_json_diagnostic_bytes())?;
                let statement = match crate::executor::json::json_value_bounded(
                    &witness,
                    crate::executor::json::JsonBounds {
                        maximum_bytes: maximum_witness_bytes,
                        maximum_items: io.remaining()?.items_examined,
                        maximum_depth: 8,
                    },
                    Some(crate::wire::ErrorCode::Storage),
                    &mut |charge| io.codec_charge(charge, false),
                ) {
                    #[cfg(not(target_arch = "wasm32"))]
                    Err(Error::NativeJson {
                        code,
                        source,
                        custody: None,
                    }) => {
                        let custody = io.take_native_custody(diagnostics.bytes())?;
                        diagnostics.into_retained();
                        return Err(Error::NativeJson {
                            code,
                            source,
                            custody: Some(custody),
                        });
                    }
                    result => result?,
                };
                root_hold.into_retained();
                drop(diagnostics);
                statement
            }
            None => {
                serde_json::to_value(witness).map_err(|error| Error::Invalid(error.to_string()))?
            }
        };
        let source_generation =
            fork_preparer_io::generation_ref(&self.host.provider, plan.source_head(), io.as_ref())?;
        let expected_target_generation =
            fork_preparer_io::generation_ref(&self.host.provider, plan.target_head(), io.as_ref())?;
        let result_generation = fork_preparer_io::generation_ref(
            &self.host.provider,
            application.generation.id(),
            io.as_ref(),
        )?;
        let receipt = crate::executor::with_history_io(io.as_ref(), |work| -> Result<_> {
            let source_project = plan.child_project.copy(work)?;
            let target_project = self.project.copy(work)?;
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let receipt = ProjectMergeReceipt {
                operation_id,
                child: crate::core::copy_authority(child, &mut quota)?,
                source_project,
                source_generation,
                target_project,
                expected_target_generation,
                result_generation,
                provider_operation_id: crate::core::copy_copy_vector(
                    &filesystem_key.into_bytes(),
                    &mut quota,
                )?,
                provider_proof: ProviderJoinProof {
                    provider: crate::core::copy_provider(&self.host.provider, &mut quota)?,
                    format: crate::core::copy_text(FILESYSTEM_JOIN_PROOF_FORMAT, &mut quota)?,
                    statement,
                },
                notice: notice.clone_in_quota(&mut quota)?,
            };
            quota.transfer();
            Ok(receipt)
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        let custody = io
            .map(|io| {
                let retained = io.owned_bytes()?;
                io.into_native_custody(retained)
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let custody = None;
        Ok((receipt, custody))
    }

    /// Describes exact three-way conflicts without yielding the publishable plan.
    pub async fn describe_project_merge_conflicts(
        &self,
        plan: &ParentMergePlan<A, O>,
        conflicts: &[MergeConflict],
        truncated: bool,
    ) -> Result<MergePlan> {
        self.require_merge_plan(plan, None)?;
        plan.plan
            .describe_conflicts(conflicts, truncated)
            .await
            .map_err(map_error)
    }

    /// Validates and publishes an admitted custom candidate under parent authority.
    pub async fn apply_project_merge_candidate(
        &self,
        plan: &ParentMergePlan<A, O>,
        operation_id: crate::OperationId,
        candidate: &acyclic_fs::UnpublishedMergeCandidate,
    ) -> Result<JoinOutcome<A, O>> {
        self.require_merge_plan(plan, None)?;
        plan.plan
            .apply_candidate(
                Self::merge_options(plan, project_join_key(operation_id, None)?),
                candidate,
                acyclic_fs::PublicationPermit::Unrestricted,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))
    }

    pub(in crate::filesystem) fn require_merge_plan(
        &self,
        plan: &ParentMergePlan<A, O>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        self.require(capability::PROJECT_MERGE, VolumeOperation::Write, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work)
            && !plan
                .native_custody
                .as_ref()
                .is_some_and(|guard| guard.work().shares_allocation_context(work))
        {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "merge plan belongs to another original parent controller",
            ));
        }
        if plan.parent_project != self.project || plan.parent_scope != self.scope {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "merge plan belongs to another parent controller",
            ));
        }
        Ok(())
    }

    pub(in crate::filesystem) fn merge_options(
        plan: &ParentMergePlan<A, O>,
        idempotency_key: FilesystemKey,
    ) -> ApplyOptions {
        ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key,
        }
    }
}

use crate::executor::{stock_boundary, stock_string_copy};
