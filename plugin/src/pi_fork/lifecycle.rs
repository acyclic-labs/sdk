//! Lifecycle of the original Pi fork.
use super::*;

impl<'a> PiForkCoordinator<'a> {
    pub(super) async fn require_original_plan(&self, plan: &PiForkPlan) -> Result<()> {
        let original =
            self.load_original_plan(plan.original)
                .await?
                .ok_or(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi preparation lost its protected original fork publication",
                })?;
        if original != *plan {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi preparation substituted its protected original fork publication",
            });
        }
        Ok(())
    }

    /// The service persists its existing Preparing route after this ordinary admission and before advance may run the provider's FS creator.
    pub(crate) async fn admit_preparation(
        &self,
        context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>,
        fence: &LeaseFence,
        plan: &PiForkPlan,
    ) -> Result<()> {
        self.authenticate(context, owner, fence).await?;
        self.validate_plan(plan, owner.task_id())?;
        self.require_original_plan(plan).await?;
        (owner)
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        self.admit_original(&plan.preparation, fence).await?;
        Ok(())
    }

    /// Drives only the retained ordinary preparation.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact parent lease and immutable source selection"
    )]
    /// Advances only the ordinary registered preparation task.
    pub(crate) async fn prepare(
        &self,
        shared: &Arc<acyclic_harness::runtime::TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        fence: &LeaseFence,
        plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>,
        budget: WorkBudget,
        cancel: CancellationToken,
    ) -> Result<Option<(ForkReport, acyclic_fs::NativeAllocation)>> {
        let context = shared.as_ref();
        let work = &self.work;
        self.authenticate(context, owner, fence).await?;
        self.validate_plan(plan, owner.task_id())?;
        self.require_original_plan(plan).await?;
        (owner)
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        cancel.check().map_err(|_| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            custody: None,
        })?;
        self.authenticate_launch(context, plan).await?;
        let preparer = self
            .native_preparer(shared, owner, plan, source, budget, &cancel, work)
            .await?;
        let host = self.runtime.runtime.task_host();
        (owner)
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        self.admit_original(&plan.preparation, fence).await?;
        if self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(plan.preparation.task_id(), &self.work)
            .await?
            .0
            != plan.preparation
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi preparation belongs to another original operation",
            });
        }
        if self
            .runtime
            .runtime
            .task_host()
            .outcome_with_native_work(plan.preparation.task_id(), &self.work)
            .await?
            .0
            .is_none()
        {
            let commands = self.runtime.runtime.commands().with_fork_preparer(preparer);
            let recovered = {
                let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
                HistoryIo::retain_owned(
                    &mut io,
                    std::mem::size_of::<WorkPull>().max(std::mem::size_of::<TaskWorkerAttempt>())
                        as u64,
                )?;
                let value = host
                    .recover_work(plan.preparation.task_id(), &mut Some(&mut io))
                    .await?;
                (value, io.into_custody()?)
            };
            match recovered.0 {
                WorkPull::Claimed(lease) | WorkPull::Unresolved { lease, .. } => {
                    let result = self
                        .runtime
                        .runtime
                        .resume_task_with_native_work(lease, recovered.1, &commands, 8, work)
                        .await?;
                    drop(result);
                }
                WorkPull::Idle => {
                    let worker_id =
                        work.display(&format_args!("pi-fork:{}", self.resources.instance_id))?;
                    let worker = Worker {
                        id: worker_id.0,
                        available: ResourceRequest::default(),
                        labels: BTreeMap::new(),
                    };
                    let result = {
                        let io = PreparationIo::from_native_work(work)?;
                        let value = self
                            .runtime
                            .runtime
                            .run_operation(
                                &worker,
                                plan.preparation.task_id(),
                                &commands,
                                8,
                                Some(&io),
                            )
                            .await?;
                        let PreparationIo::Native { custody, .. } = io else {
                            unreachable!()
                        };
                        (
                            value,
                            custody
                                .into_inner()
                                .unwrap_or_else(std::sync::PoisonError::into_inner),
                        )
                    };
                    drop(result);
                }
            }
        }
        let outcome = self
            .runtime
            .runtime
            .task_host()
            .outcome_with_native_work(plan.preparation.task_id(), &self.work)
            .await?;
        let report: (ForkReport, acyclic_fs::NativeAllocation) = match &outcome.0 {
            Some(Outcome::Succeeded(value)) => self.decode_input_value(
                value,
                acyclic_harness::JsonValueSchema::ForkReport,
                FORK_PLAN_DIAGNOSTIC_LABEL_BYTES,
            )?,
            Some(Outcome::Failed { .. }) => {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi original preparation failed",
                });
            }
            Some(Outcome::Cancelled) => {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi preparation cancelled without admitting a child",
                });
            }
            None | Some(Outcome::Indeterminate { .. }) => return Ok(None),
        };
        report.0.validate_with_native_work(work)?;
        if !self.same_payload(&plan.request.payload, &report.0.request)? {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi report changed the retained original request",
            });
        }
        (owner)
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        Ok(Some(report))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact parent lease and immutable source selection"
    )]
    pub(crate) async fn advance(
        &self,
        shared: &Arc<acyclic_harness::runtime::TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        fence: &LeaseFence,
        plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>,
        budget: WorkBudget,
        cancel: CancellationToken,
    ) -> Result<Option<PreparedPiFork>> {
        let context = shared.as_ref();
        let Some(report) = self
            .prepare(
                shared,
                owner,
                fence,
                plan,
                source.clone(),
                self.remaining(budget)?,
                cancel.clone(),
            )
            .await?
        else {
            return Ok(None);
        };
        let host = self.runtime.runtime.task_host();
        (owner)
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        cancel.check().map_err(|_| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            custody: None,
        })?;
        self.admit_original(&plan.request.child, fence).await?;
        let admission = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(plan.request.child.task_id(), &self.work)
            .await?;
        if admission.0 != plan.request.child {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi child admission differs from its actual creator request",
            });
        }
        (owner)
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        self.publish(
            context,
            owner,
            plan,
            &report.0,
            source.clone(),
            self.remaining(budget)?,
            &cancel,
        )
        .await?;
        let prepared = self
            .created_fork(
                plan,
                report.0,
                admission.0,
                source,
                &cancel,
                report.1,
                admission.1,
            )
            .await?;
        (owner)
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        Ok(Some(prepared))
    }

    /// Reopens only a previously published and bound admitted child.
    pub(crate) async fn restore_admitted(
        &self,
        shared: &Arc<acyclic_harness::runtime::TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        fence: &LeaseFence,
        plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>,
        cancel: CancellationToken,
    ) -> Result<PreparedPiFork> {
        let context = shared.as_ref();
        let admission = self.authenticate(context, owner, fence).await?;
        self.validate_plan(plan, plan.commit.caller)?;
        if owner.task_id() != plan.request.child.task_id() || admission.0 != plan.request.child {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi cold restore does not own the original admitted child",
            });
        }
        cancel.check().map_err(|_| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            custody: None,
        })?;
        let prepared = self
            .observe_prepared(plan, source, cancel)
            .await?
            .ok_or_else(|| Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi cold child has no successful published original creator",
            })?;
        (owner)
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        Ok(prepared)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "actual report and admission ownership precede their original guards"
    )]
    pub(super) async fn created_fork(
        &self,
        plan: &PiForkPlan,
        report: ForkReport,
        admission: TaskAdmissionRecord,
        source: Arc<LocalLazyWorkspace>,
        cancel: &CancellationToken,
        report_allocation: acyclic_fs::NativeAllocation,
        mut admission_allocation: acyclic_fs::NativeAllocation,
    ) -> Result<PreparedPiFork> {
        let work = &self.work;
        let project_name = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let value = report
                .request
                .preparation
                .child_project_volume
                .storage_name(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let project = self
            .resources
            .fs
            .open_workspace_with_native_work(&project_name.0, work, cancel)
            .await
            .map_err(native_workspace_error)?;
        let generation = report
            .captures
            .iter()
            .find_map(|capture| match capture {
                acyclic_harness::fork::Capture::Captured(resource) => match &resource.revision {
                    acyclic_harness::fork::ResourceRevision::Project { volume, generation }
                        if volume == &report.request.preparation.child_project_volume =>
                    {
                        Some(generation)
                    }
                    _ => None,
                },
                _ => None,
            })
            .ok_or(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi creator report lost its exact child project generation",
            })?;
        let generation: [u8; 32] =
            generation
                .as_resource()
                .key()
                .try_into()
                .map_err(|_| Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Invalid,
                    reason: "Pi created project generation has a different identity shape",
                })?;
        let (workspace, native_lazy) = source
            .bind_prepared_fork_with_native_work(
                plan.frozen,
                project,
                GenerationId::new(Digest::from_bytes(generation)),
                plan.project_creation,
                work,
                cancel,
            )
            .await
            .map_err(crate::pi_fork_host::native_lazy_error)?;
        let scratch_name = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let value = report
                .child_private_volume
                .storage_name(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let scratch = self
            .resources
            .fs
            .open_workspace_with_native_work(&scratch_name.0, work, cancel)
            .await
            .map_err(native_workspace_error)?;
        let mut binding = self.native_copy::<PiTaskBinding, _>(
            &plan.commit.child,
            acyclic_harness::JsonValueSchema::None,
            work,
        )?;
        let mut scratch_generation = self
            .native_copy::<acyclic_harness::resources::GenerationRef, _>(
                &report.child_private_generation,
                acyclic_harness::JsonValueSchema::None,
                work,
            )?;
        binding.0.scratch.allocation = PiScratchAllocation::Created(PiScratchCreation {
            workspace_id: scratch.id().into_bytes(),
            generation: scratch_generation.0,
        });
        binding.1.absorb(&mut scratch_generation.1)?;
        let result_binding = binding.0.results.as_mut().ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi actual child lost its unmounted result intent",
        })?;
        if report.child_result_volume.as_ref() != Some(&result_binding.volume) {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi creator report changed its exact child result volume",
            });
        }
        let generation = report
            .child_result_generation
            .as_ref()
            .ok_or(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi creator report lost its actual result generation",
            })?;
        let result_name = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let value = result_binding.volume.storage_name(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let results = self
            .resources
            .fs
            .open_workspace_with_native_work(&result_name.0, work, cancel)
            .await
            .map_err(native_workspace_error)?;
        let mut generation = self.native_copy::<acyclic_harness::resources::GenerationRef, _>(
            generation,
            acyclic_harness::JsonValueSchema::None,
            work,
        )?;
        result_binding.allocation = PiScratchAllocation::Created(PiScratchCreation {
            workspace_id: results.id().into_bytes(),
            generation: generation.0,
        });
        binding.1.absorb(&mut generation.1)?;
        binding.0.validate_with_native_work(
            None,
            binding.0.project_volumes.keys().map(String::as_str),
            false,
            work,
        )?;
        let workspace = workspace
            .into_arc_with_native_work(work)
            .map_err(crate::pi_fork_host::native_lazy_error)?;
        let mut prepared = PreparedPiFork {
            binding: binding.0,
            report,
            admission,
            frozen_source: plan.frozen,
            native_lazy,
            workspace,
            _metadata_custody: report_allocation,
        };
        prepared
            ._metadata_custody
            .absorb(&mut admission_allocation)?;
        prepared._metadata_custody.absorb(&mut binding.1)?;
        return Ok(prepared);
    }

    pub(super) async fn publish(
        &self,
        context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>,
        plan: &PiForkPlan,
        report: &ForkReport,
        source: Arc<LocalLazyWorkspace>,
        budget: WorkBudget,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let work = &self.work;
        let admission = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(owner.task_id(), &self.work)
            .await?;
        let parent_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(
            &admission.0.input,
            acyclic_harness::JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
        )?;
        let authority = self.owner.conversation_authority_with_native_work(
            owner.task_id(),
            parent_input.0.binding.agent_id,
            context.scope().grants(),
            work,
        )?;
        let verifier = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let value = authority.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let mut seed_io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
        seed_io.retain_owned(std::mem::size_of::<ForkReport>() as u64)?;
        let (seed, _) = report.copy(&mut Some(&mut seed_io))?;
        let seed_value = seed.into_seed(&mut Some(&mut seed_io))?;
        let _seed_custody = seed_io.into_custody()?;
        source
            .validate_snapshot_with_native_work(plan.frozen, work)
            .await
            .map_err(crate::pi_fork_host::native_lazy_error)?;
        let filesystem: Arc<dyn acyclic_harness::fork::ForkSeedVerifier> =
            acyclic_harness::filesystem::FilesystemForkVerifier::new_with_native_work(
                self.filesystem.clone(),
                plan.request.maximum_bytes,
                &verifier.0,
                &authority.0.2,
                work,
            )?
            .into_native_shared(work)?;
        let stream_provider = acyclic_harness::resources::ProviderRef::new_with_native_work(
            "local", "stream", "1", work,
        )?;
        let history: Arc<dyn acyclic_harness::fork::ForkSeedVerifier> =
            acyclic_harness::fork::StreamHistoryForkVerifier::new_with_native_work(
                &stream_provider.0,
                work,
            )?
            .into_native_shared(work)?;
        let forks = acyclic_harness::fork::CompositeForkVerifier::new_with_native_work(
            &[filesystem, history],
            work,
        )?
        .into_native_shared(work)?;
        let parent = {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            io.preflight()?;
            let aggregate = StreamAggregate::open(
                &self.stream,
                authority.0.0,
                verifier.0,
                SchemaRegistry::new(),
                None,
                acyclic_harness::store::default_projection_read_limits(),
                acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut Some(&mut io),
            )
            .await?;
            io.return_staged_output()?;
            (aggregate, io.into_custody()?)
        };
        let mut parent_value = parent.0;
        parent_value.set_limits(context.scope().limits())?;
        parent_value = parent_value.with_fork_verifier(forks);
        {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            io.retain_owned(std::mem::size_of::<acyclic_harness::core::ApplyResult>() as u64)?;
            let (report, report_bytes) = report.copy(&mut Some(&mut io))?;
            let mut scope_io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let scope = authority.0.2.copy(&mut Some(&mut scope_io))?;
            let scope_custody = scope_io.into_custody()?;
            let scope_bytes = scope_custody.bytes();
            io.absorb_native_custody(scope_custody)?;
            let published = parent_value
                .publish_fork_report(report, scope, Some(owner), &mut Some(&mut io))
                .await;
            let released_report = io.release_owned(report_bytes);
            let released_scope = io.release_owned(scope_bytes);
            let published = published?;
            released_report?;
            released_scope?;
            drop(published);
            drop(io.into_custody()?);
        }
        let child_authority = self.owner.conversation_authority_with_native_work(
            plan.request.child.task_id(),
            seed_value.child_agent,
            &plan.request.child.grants,
            work,
        )?;
        let child_verifier = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let value = child_authority.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let child = {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            io.preflight()?;
            let aggregate = StreamAggregate::open(
                &self.stream,
                child_authority.0.0,
                child_verifier.0,
                SchemaRegistry::new(),
                None,
                acyclic_harness::store::default_projection_read_limits(),
                acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut Some(&mut io),
            )
            .await?;
            io.return_staged_output()?;
            (aggregate, io.into_custody()?)
        };
        let mut child_value = child.0;
        child_value.set_limits(plan.request.child.limits)?;
        {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let mut scope_io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let scope = child_authority.0.2.copy(&mut Some(&mut scope_io))?;
            let scope_custody = scope_io.into_custody()?;
            let scope_bytes = scope_custody.bytes();
            io.absorb_native_custody(scope_custody)?;
            let bound = child_value
                .bind_published_child(
                    &parent_value,
                    &seed_value,
                    scope,
                    Some(owner),
                    &mut Some(&mut io),
                )
                .await;
            let released = io.release_owned(scope_bytes);
            bound?;
            released?;
        }
        return Ok(());
    }
}
