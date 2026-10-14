//! Allocation of the original Pi fork.
use super::*;
use acyclic_harness::filesystem::PreparationIo;

pub(super) fn project_operation(
    request: &ForkRequest,
    maximum: u64,
    work: &NativeMaterializationWork,
) -> Result<acyclic_fs::OperationId> {
    let bytes = crate::pi_root_provider::guarded_pi_bytes(request, maximum, work)?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: bytes.0.len() as u64,
        ..acyclic_fs::WorkCounters::UNCHARGED
    })?;
    let digest = blake3::hash(&bytes.0);
    let key = work.display(&format_args!(
        "fork:{}:{digest}:project",
        request.operation_id
    ))?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: key.0.len() as u64,
        ..acyclic_fs::WorkCounters::UNCHARGED
    })?;
    let key = acyclic_harness::IdempotencyKey::new(key.0)?;
    Ok(acyclic_fs::OperationId::from_bytes(
        acyclic_harness::filesystem::filesystem_key(&key).into_bytes(),
    ))
}

impl<'a> PiForkCoordinator<'a> {
    /// Observes genuine completed preparation, admission and historical bind.
    pub(crate) fn observe_prepared<'b>(
        &'b self,
        plan: &'b PiForkPlan,
        source: Arc<LocalLazyWorkspace>,
        cancel: CancellationToken,
    ) -> impl std::future::Future<Output = Result<Option<PreparedPiFork>>> + Send + 'b {
        async move {
            self.require_original_plan(plan).await?;
            {
                let work = &self.work;
                let preparation = match self
                    .runtime
                    .runtime
                    .task_host()
                    .observe_admission_with_native_work(plan.preparation.task_id(), &self.work)?
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
                    .outcome_with_native_work(plan.preparation.task_id(), &self.work)?
                    .await?;
                let report: (ForkReport, acyclic_fs::NativeAllocation) = match &outcome.0 {
                    Some(Outcome::Succeeded(value)) => self.decode_input_value(
                        value,
                        acyclic_harness::JsonValueSchema::ForkReport,
                        FORK_PLAN_DIAGNOSTIC_LABEL_BYTES,
                    )?,
                    _ => return Ok(None),
                };
                report
                    .0
                    .validate(&mut Some(&mut &PreparationIo::from_native_work(work)?))?;
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
                    .observe_admission_with_native_work(plan.request.child.task_id(), &self.work)?
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
                self.authenticate_launch(plan).await?;
                let parent_admission = self
                    .runtime
                    .runtime
                    .task_host()
                    .observe_admission_with_native_work(plan.commit.caller, &self.work)?
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
                    let mut io = acyclic_harness::store::HistoryWork::new(
                        PreparationIo::from_native_work(work)?,
                    );
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
                    let mut io = acyclic_harness::store::HistoryWork::new(
                        PreparationIo::from_native_work(work)?,
                    );
                    let value = child_authority.0.1.verifier(&mut Some(&mut io))?;
                    (value, io.into_custody()?)
                };
                let mut io = acyclic_harness::store::HistoryWork::new(
                    PreparationIo::from_native_work(work)?,
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
                let seed = report.0.copy(&mut Some(&mut io))?;
                let seed_value = seed.0.into_seed(&mut Some(&mut io))?;
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
    }

    pub(super) async fn prepare_native_source(
        &self,
        owner: &TaskJournalOwner<LocalStream>,
        plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>,
    ) -> Result<()> {
        (owner)
            .verify(
                false,
                &mut Some(&mut &PreparationIo::from_native_work(&self.work)?),
            )
            .await?;
        {
            let work = &self.work;
            let request: (ForkRequest, acyclic_fs::NativeAllocation) = self.decode_input_value(
                &plan.request.payload,
                acyclic_harness::JsonValueSchema::ForkRequest,
                FORK_PLAN_DIAGNOSTIC_LABEL_BYTES,
            )?;
            let name = {
                let mut io = acyclic_harness::store::HistoryWork::new(
                    PreparationIo::from_native_work(work)?,
                );
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
                    project_operation(&request.0, plan.request.maximum_bytes, work)?,
                    &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                        work,
                        cancellation: work.cancellation(),
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
                .verify(
                    false,
                    &mut Some(&mut &PreparationIo::from_native_work(&self.work)?),
                )
                .await?;
            Ok(())
        }
    }

    pub(super) async fn native_preparer(
        &self,
        shared: &Arc<acyclic_harness::runtime::TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>,
    ) -> Result<
        Arc<
            acyclic_harness::filesystem::FilesystemForkPreparer<
                LocalAuthorityBackend,
                LocalObjectBackend,
            >,
        >,
    > {
        let context = shared.as_ref();
        let work = &self.work;
        let admission = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(owner.task_id(), &self.work)?
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
        let request: (ForkRequest, acyclic_fs::NativeAllocation) = self.decode_input_value(
            &plan.request.payload,
            acyclic_harness::JsonValueSchema::ForkRequest,
            FORK_PLAN_DIAGNOSTIC_LABEL_BYTES,
        )?;
        let checkpoint = context
            .read_file(
                plan.request
                    .source_checkpoint
                    .as_ref()
                    .ok_or(Error::NativeBoundary {
                        code: acyclic_harness::wire::ErrorCode::Unauthorized,
                        reason: "Pi fork plan lacks its original source checkpoint",
                    })?,
                Some(&PreparationIo::from_native_work(work)?),
            )
            .await?;
        (owner)
            .verify(
                false,
                &mut Some(&mut &PreparationIo::from_native_work(&self.work)?),
            )
            .await?;
        let mut snapshot_io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        let snapshot = Snapshot::decode(
            checkpoint.as_ref(),
            plan.request.maximum_bytes,
            &mut Some(&mut snapshot_io),
        )?;
        if snapshot.authority != request.0.parent || snapshot.revision != request.0.parent_revision
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi original checkpoint differs from its canonical parent revision",
            });
        }
        let verifier = {
            let mut io =
                acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
            let value = authority.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let reducer = Reducer::restore(
            snapshot,
            verifier.0,
            SchemaRegistry::new(),
            &mut Some(&mut snapshot_io),
        )?;
        snapshot_io.absorb_native_custody(verifier.1)?;
        let source_read = match &request.0.authorized_source {
            Some(authorization) => {
                let io = PreparationIo::from_native_work(work)?;
                let handle = owner
                    .restore_project_source_authorization(context, authorization, Some(&io))
                    .await?;
                Some((
                    handle,
                    acyclic_harness::store::HistoryWork::new(io).into_custody()?,
                ))
            }
            None => None,
        };
        let resolver = acyclic_harness::runtime::content_residency_verifier(
            std::sync::Arc::clone(shared),
            owner.clone(),
            Some(&PreparationIo::from_native_work(work)?),
        )?;
        let verifier = {
            let mut io =
                acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
            let value = authority.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let mut provider_io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        let reference = acyclic_harness::resources::ProviderRef::new(
            provider_io.clone_text("local")?,
            provider_io.clone_text("stream")?,
            provider_io.clone_text("1")?,
            &mut Some(&mut provider_io),
        )?;
        let provider = (reference, provider_io.into_custody()?);
        let restored = acyclic_harness::filesystem::restore_recursive_fork_source(
            self.filesystem.clone(),
            &self.stream,
            (
                reducer,
                Some(PreparationIo::from_native_custody(
                    snapshot_io.into_custody()?,
                )),
            ),
            &verifier.0,
            &authority.0.2,
            caller_project,
            &provider.0,
            request.0,
            plan.history_limits(),
            source_read.as_ref().map(|pair| &pair.0),
            resolver,
            work.cancellation(),
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
        let preparer = restored.preparer;
        (owner)
            .verify(
                false,
                &mut Some(&mut &PreparationIo::from_native_work(&self.work)?),
            )
            .await?;
        Ok(preparer)
    }
}
