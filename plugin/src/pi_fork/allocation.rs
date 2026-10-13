//! Allocation of the original Pi fork.
use super::*;

impl<'a> PiForkCoordinator<'a> {
    /// Observes genuine completed preparation, admission and historical bind.
    pub(crate) async fn observe_prepared(
        &self,
        plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>,
        cancel: CancellationToken,
    ) -> Result<Option<PreparedPiFork>> {
        let original = self
            .load_original_plan(plan.original)
            .await?
            .ok_or_else(|| Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi prepared observation lost its original immutable plan",
            })?;
        if original != *plan {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi prepared observation substituted its original plan",
            });
        }
        {
            let work = &self.work;
            let preparation = match self
                .runtime
                .runtime
                .task_host()
                .observe_admission_with_native_work(plan.preparation.task_id(), &self.work)
                .await
            {
                Ok(value) => value,
                Err(Error::NotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            if preparation.0 != plan.preparation {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi original preparation admission changed",
                });
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
                _ => return Ok(None),
            };
            report.0.validate_with_native_work(work)?;
            if !self.same_payload(&plan.request.payload, &report.0.request)? {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi actual creator report changed its original request",
                });
            }
            let admission = match self
                .runtime
                .runtime
                .task_host()
                .observe_admission_with_native_work(plan.request.child.task_id(), &self.work)
                .await
            {
                Ok(value) => value,
                Err(Error::NotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            if admission.0 != plan.request.child {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi original child admission changed",
                });
            }
            let reference = crate::pi_root_provider::observe_pi_input(
                &self.owner,
                &self.filesystem,
                derived(plan.original, "native-input", &self.work)?,
                "native-input",
                &self.work,
            )
            .await?
            .ok_or(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi original native input disappeared",
            })?;
            let child: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(
                &admission.0.input,
                acyclic_harness::JsonValueSchema::None,
                crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
            )?;
            let PiTaskExecution::Child { launch } = &child.0.execution else {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi admitted child substituted Root execution",
                });
            };
            let bytes = self.read_original_input(&reference.0).await?;
            let intent: (PiNativeLaunchIntent, acyclic_fs::NativeAllocation) = self
                .decode_original_input(
                    bytes.as_ref(),
                    acyclic_harness::JsonValueSchema::None,
                    crate::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES,
                )?;
            if launch.payload != reference.0 || intent.0 != plan.commit.launch {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi admitted child changed its sealed native intent",
                });
            }
            let parent_admission = self
                .runtime
                .runtime
                .task_host()
                .observe_admission_with_native_work(plan.commit.caller, &self.work)
                .await?;
            let parent_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self
                .decode_input_value(
                    &parent_admission.0.input,
                    acyclic_harness::JsonValueSchema::None,
                    crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
                )?;
            let parent_authority = self.owner.conversation_authority_with_native_work(
                plan.commit.caller,
                parent_input.0.binding.agent_id,
                &parent_admission.0.grants,
                work,
            )?;
            let parent_verifier = {
                let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
                let value = parent_authority.0.1.verifier(&mut Some(&mut io))?;
                (value, io.into_custody()?)
            };
            let child_authority = self.owner.conversation_authority_with_native_work(
                plan.request.child.task_id(),
                plan.commit.child.agent_id,
                &admission.0.grants,
                work,
            )?;
            let child_verifier = {
                let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
                let value = child_authority.0.1.verifier(&mut Some(&mut io))?;
                (value, io.into_custody()?)
            };
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            let parent = acyclic_harness::store::HistoryReader::new(
                &self.stream,
                &parent_authority.0.0,
                &parent_verifier.0,
                &mut Some(&mut io),
            )?;
            let child = acyclic_harness::store::HistoryReader::new(
                &self.stream,
                &child_authority.0.0,
                &child_verifier.0,
                &mut Some(&mut io),
            )?;
            let seed = report.0.clone_with_native_work(work)?;
            let seed_value = seed.0.into_seed()?;
            child
                .verify_published_child_with_work(
                    &parent,
                    &seed_value,
                    plan.history_limits(),
                    &mut Some(&mut io),
                )
                .await?;
            source
                .validate_snapshot(
                    plan.frozen,
                    &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                        work,
                        cancellation: work.cancellation(),
                    },
                )
                .await
                .map_err(crate::pi_fork_host::native_lazy_error)?;
            return Ok(Some(
                self.created_fork(
                    plan,
                    report.0,
                    admission.0,
                    source,
                    &cancel,
                    report.1,
                    admission.1,
                )
                .await?,
            ));
        }
    }

    pub(super) async fn prepare_native_source(
        &self,
        owner: &TaskJournalOwner<LocalStream>,
        plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>,
        cancel: &CancellationToken,
    ) -> Result<()> {
        (owner)
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        {
            let work = &self.work;
            let request: (ForkRequest, acyclic_fs::NativeAllocation) = self.decode_input_value(
                &plan.request.payload,
                acyclic_harness::JsonValueSchema::ForkRequest,
                FORK_PLAN_DIAGNOSTIC_LABEL_BYTES,
            )?;
            let name = {
                let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
                let value = request
                    .0
                    .preparation
                    .child_project_volume
                    .storage_name(&mut Some(&mut io))?;
                (value, io.into_custody()?)
            };
            let prepared = source
                .prepare_fork_snapshot(
                    &name.0,
                    plan.frozen,
                    plan.project_creation,
                    &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                        work,
                        cancellation: cancel,
                    },
                )
                .await
                .map_err(crate::pi_fork_host::native_lazy_error)?;
            if prepared != plan.frozen {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "native preparation changed its immutable source cut",
                });
            }
            (owner)
                .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
                .await?;
            return Ok(());
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "original native owner, source cut, and invocation work stay distinct"
    )]
    pub(super) async fn native_preparer(
        &self,
        shared: &Arc<acyclic_harness::runtime::TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>,
        budget: WorkBudget,
        cancel: &CancellationToken,
        work: &NativeMaterializationWork,
    ) -> Result<
        Arc<
            acyclic_harness::filesystem::FilesystemForkPreparer<
                LocalAuthorityBackend,
                LocalObjectBackend,
            >,
        >,
    > {
        let context = shared.as_ref();
        let admission = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(owner.task_id(), &self.work)
            .await?;
        let caller: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(
            &admission.0.input,
            acyclic_harness::JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
        )?;
        let caller_project = plan
            .commit
            .child
            .project_volumes
            .keys()
            .next()
            .and_then(|key| caller.0.binding.project_volumes.get(key))
            .or_else(|| caller.0.binding.project_volumes.values().next())
            .ok_or(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi caller has no actual project binding",
            })?;
        let authority = self.owner.conversation_authority_with_native_work(
            owner.task_id(),
            caller.0.binding.agent_id,
            context.scope().grants(),
            work,
        )?;
        let request: (ForkRequest, acyclic_fs::NativeAllocation) = self.native_copy(
            &plan.request.payload,
            acyclic_harness::JsonValueSchema::ForkRequest,
            work,
        )?;
        let checkpoint = context
            .read_file_with_native_work(
                plan.request
                    .source_checkpoint
                    .as_ref()
                    .ok_or(Error::NativeBoundary {
                        code: acyclic_harness::wire::ErrorCode::Unauthorized,
                        reason: "Pi fork plan lacks its original source checkpoint",
                    })?,
                work,
            )
            .await?;
        (owner)
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        let snapshot = Snapshot::decode_with_native_work(
            checkpoint.as_ref(),
            plan.request.maximum_bytes,
            work,
        )?;
        if snapshot.authority != request.0.parent || snapshot.revision != request.0.parent_revision
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi original checkpoint differs from its canonical parent revision",
            });
        }
        let verifier = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let value = authority.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let reducer = Reducer::restore_native(snapshot, verifier.0, SchemaRegistry::new(), work)?;
        let source_read = match &request.0.authorized_source {
            Some(authorization) => Some(
                owner
                    .restore_project_source_authorization_with_native_work(
                        context,
                        authorization,
                        work,
                    )?
                    .await?,
            ),
            None => None,
        };
        let resolver = acyclic_harness::runtime::content_residency_verifier(
            std::sync::Arc::clone(shared),
            owner.clone(),
            Some(&acyclic_harness::filesystem::PreparationIo::from_native_work(work)?),
        )?;
        let verifier = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let value = authority.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let provider = acyclic_harness::resources::ProviderRef::new_with_native_work(
            "local", "stream", "1", work,
        )?;
        let restored = acyclic_harness::filesystem::restore_recursive_fork_source_with_native_work(
            self.filesystem.clone(),
            &self.stream,
            reducer,
            &verifier.0,
            &authority.0.2,
            caller_project,
            &provider.0,
            request,
            plan.history_limits(),
            source_read.as_ref().map(|pair| &pair.0),
            resolver,
            cancel,
            work,
        )
        .await?;
        source
            .validate_snapshot(
                plan.frozen,
                &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                    work,
                    cancellation: work.cancellation(),
                },
            )
            .await
            .map_err(crate::pi_fork_host::native_lazy_error)?;
        let preparer = restored.0.preparer;
        if preparer.project_creation_operation(&restored.0.request)? != plan.project_creation {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native companion changed its original filesystem operation",
            });
        }
        (owner)
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        Ok(preparer)
    }
}
