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
        self.require(capability::FORK_PUBLISH, VolumeOperation::Read)?;
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
            .map(|custody| {
                self.require_with_native_work(
                    capability::PROJECT_MERGE,
                    VolumeOperation::Write,
                    custody.work(),
                )
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io: Option<PreparationIo> = None;
        if io.is_none() {
            self.require(capability::PROJECT_MERGE, VolumeOperation::Write)?;
        }
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
                let io = self.require_with_native_work(
                    capability::PROJECT_MERGE,
                    VolumeOperation::Write,
                    custody.work(),
                )?;
                source.validate(&mut Some(&mut io.history_io()))?;
                if source.provider() != &self.host.provider
                    || (!selected
                        && !io.contains_capability(
                            self.scope.capabilities(),
                            source,
                            "volume:read:",
                        )?)
                {
                    return Err(fork_preparer_io::failure(
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
                self.require(capability::PROJECT_MERGE, VolumeOperation::Write)?;
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

    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) fn require_merge_plan_with_native_work(
        &self,
        plan: &ParentMergePlan<A, O>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<PreparationIo> {
        let io =
            self.require_with_native_work(capability::PROJECT_MERGE, VolumeOperation::Write, work)?;
        if !plan
            .native_custody
            .as_ref()
            .is_some_and(|guard| guard.work().shares_allocation_context(work))
            || plan.parent_project != self.project
            || plan.parent_scope != self.scope
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "merge plan belongs to another original parent controller",
            });
        }
        Ok(io)
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
            let io = self.require_merge_plan_with_native_work(plan, work)?;
            let binding =
                NativeFilesystemVerificationBinding::new(&self.verifier, &self.scope, work)?;
            let project = self.project.copy(&mut Some(&mut io.history_io()))?;
            let bytes = std::mem::size_of::<(
                NativeFilesystemVerificationBinding,
                VolumeRef,
                acyclic_fs::NativeAllocation,
            )>() as u64;
            io.retain_owned(bytes)?;
            io.allocation_attempt(bytes)?;
            let retained = io.owned_bytes()?;
            let owner = Arc::new((binding, project, io.into_native_custody(retained)?));
            let mut guard = move |original: &acyclic_fs::NativeMaterializationWork| {
                std::future::ready((|| {
                    if !owner.2.work().shares_allocation_context(original) {
                        return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unauthorized,
                            reason: "merge publication owner belongs to another original resource",
                        });
                    }
                    owner.0.authenticate(original)?;
                    let io = PreparationIo::from_native_work(original)?;
                    if !owner
                        .0
                        .scope
                        .capabilities()
                        .contains(capability::PROJECT_MERGE)
                        || !io.contains_capability(
                            owner.0.scope.capabilities(),
                            &owner.1,
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
        self.require_merge_plan(plan)?;
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
        if let Some(custody) = &self.native_custody {
            let work = custody.work();
            self.require_merge_plan_with_native_work(plan, work)?;
            let (JoinOutcome::Applied(application) | JoinOutcome::AlreadyApplied(application)) =
                outcome
            else {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "project join did not publish a result",
                });
            };
            let io = PreparationIo::from_native_work(work)?;
            let filesystem_key =
                project_join_key(operation_id, Some(&PreparationIo::from_native_work(work)?))?;
            let _witness = plan
                .plan
                .commit_witness(application, filesystem_key, work, io.cancellation())
                .map_err(|source| Error::NativeWorkspace {
                    source,
                    custody: None,
                })?;
            let maximum_witness_bytes = (std::mem::size_of::<JoinCommitWitness>() as u64)
                .checked_mul(16)
                .and_then(|bytes| bytes.checked_add(1024))
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "join witness bound overflows",
                })?;
            let root = std::mem::size_of::<serde_json::Value>() as u64;
            io.retain_owned(root)?;
            let root_hold = io.guard_owned(root)?;
            let diagnostics = io.reserve_decode(crate::contract::sdk_json_diagnostic_bytes())?;
            let statement = match crate::contract::json_value_bounded(
                &_witness,
                crate::contract::JsonBounds {
                    maximum_bytes: maximum_witness_bytes,
                    maximum_items: io.remaining()?.items_examined,
                    maximum_depth: 8,
                },
                Some(crate::wire::ErrorCode::Storage),
                &mut |charge| io.codec_charge(charge, false),
            ) {
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
            let mut observer = io.history_io();
            let child = crate::store::history::HistoryIo::clone_authority(&mut observer, child)?;
            drop(observer);
            let source_project = plan.child_project.copy(&mut Some(&mut io.history_io()))?;
            let target_project = self.project.copy(&mut Some(&mut io.history_io()))?;
            let (source_generation, custody) = GenerationRef::new(&self.host.provider, &plan.source_head().digest().into_bytes(), None, Some(work)).map(|(value, custody)| (value, custody.expect("native generation custody")))?;
            io.absorb_native_custody(custody)?;
            let (expected_target_generation, custody) = GenerationRef::new(&self.host.provider, &plan.target_head().digest().into_bytes(), None, Some(work)).map(|(value, custody)| (value, custody.expect("native generation custody")))?;
            io.absorb_native_custody(custody)?;
            let result_generation = fork_preparer_io::generation_ref(
                &self.host.provider,
                application.generation.id(),
                Some(&io),
            )?;
            let operation_bytes = filesystem_key.into_bytes();
            let mut provider_operation_id = Vec::new();
            fork_preparer_io::reserve(
                &mut provider_operation_id,
                operation_bytes.len(),
                Some(&io),
            )?;
            let copy = acyclic_fs::WorkCounters {
                bytes_copied: operation_bytes.len() as u64,
                ..Default::default()
            };
            io.preflight(copy)?;
            io.charge(copy)?;
            provider_operation_id.extend_from_slice(&operation_bytes);
            let provider = self.host.provider.clone_with_io(&io)?;
            let format = fork_preparer_io::clone_text(FILESYSTEM_JOIN_PROOF_FORMAT, Some(&io))?;
            let mut observer = io.history_io();
            let mut history_work = Some(&mut observer as &mut dyn crate::store::history::HistoryIo);
            let mut quota = crate::wire_codec::CodecQuota::new(&mut history_work, 0)?;
            let notice = notice.clone_in_quota(&mut quota)?;
            quota.transfer();
            drop(quota);
            let receipt = ProjectMergeReceipt {
                operation_id,
                child,
                source_project,
                source_generation,
                target_project,
                expected_target_generation,
                result_generation,
                provider_operation_id,
                provider_proof: ProviderJoinProof {
                    provider,
                    format,
                    statement,
                },
                notice,
            };
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            return Ok((receipt, Some(custody)));
        }
        self.require_merge_plan(plan)?;
        let (JoinOutcome::Applied(application) | JoinOutcome::AlreadyApplied(application)) =
            outcome
        else {
            return Err(Error::Conflict(
                "project join did not publish a result".into(),
            ));
        };
        let filesystem_key = project_join_key(operation_id, None)?;
        let witness = plan
            .plan
            .commit_witness(
                application,
                filesystem_key,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error))?;
        let receipt = ProjectMergeReceipt {
            operation_id,
            child: child.clone(),
            source_project: plan.child_project.clone(),
            source_generation: self.host.generation_ref_id(plan.source_head())?,
            target_project: self.project.clone(),
            expected_target_generation: self.host.generation_ref_id(plan.target_head())?,
            result_generation: self.host.generation_ref_id(application.generation.id())?,
            provider_operation_id: filesystem_key.into_bytes().to_vec(),
            provider_proof: ProviderJoinProof {
                provider: self.host.provider.clone(),
                format: FILESYSTEM_JOIN_PROOF_FORMAT.into(),
                statement: serde_json::to_value(witness)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            },
            notice: notice.clone(),
        };
        Ok((receipt, None))
    }

    /// Describes exact three-way conflicts without yielding the publishable plan.
    pub async fn describe_project_merge_conflicts(
        &self,
        plan: &ParentMergePlan<A, O>,
        conflicts: &[MergeConflict],
        truncated: bool,
    ) -> Result<MergePlan> {
        self.require_merge_plan(plan)?;
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
        self.require_merge_plan(plan)?;
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
    ) -> Result<()> {
        self.require(capability::PROJECT_MERGE, VolumeOperation::Write)?;
        if plan.parent_project != self.project || plan.parent_scope != self.scope {
            return Err(Error::Unauthorized(
                "merge plan belongs to another parent controller".into(),
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
