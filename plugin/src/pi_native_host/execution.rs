//! Native commands over the existing task runtime.
use super::*;

pub(crate) async fn recover_task(
    runtime: &PiTaskRuntime,
    task: TaskId,
    work: &NativeMaterializationWork,
) -> Result<(WorkPull, NativeAllocation)> {
    let mut io = acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
    io.retain_owned(
        std::mem::size_of::<WorkPull>().max(std::mem::size_of::<TaskWorkerAttempt>()) as u64,
    )?;
    let recovered = runtime
        .runtime
        .task_host()
        .recover_work(task, &mut Some(&mut io))
        .await?;
    Ok((recovered, io.into_custody()?))
}

impl PiNativeCommandHost {
    /// Run this future in a service background job, never the session actor.
    pub(crate) async fn run_child(
        &self,
        task: TaskId,
    ) -> Result<(Option<TaskWorkerAttempt>, NativeAllocation)> {
        let entry = self.registered_entry(task)?;
        let _run = entry.run.lock().await;
        // Each run is its own invocation on the task's session ledger.
        let work = self.preparation_work(&entry).await?.invocation_window()?;
        let observed = self.resume_claim(task, &work).await?;
        if observed.0.is_some() {
            return Ok(observed);
        }
        drop(observed.1);
        let (id, custody) =
            crate::guarded_cli_text(&["pi-native:", &self.resources.instance_id], &work)?;
        let worker = Worker {
            id,
            available: ResourceRequest::default(),
            labels: BTreeMap::new(),
        };
        let result = run_operation(&self.runtime, &worker, task, self, &work).await;
        drop(worker);
        drop(custody);
        result
    }

    /// Resume only a reservation already retained by the original task host.
    pub(crate) async fn recover_child(
        &self,
        task: TaskId,
    ) -> Result<(Option<TaskWorkerAttempt>, NativeAllocation)> {
        let entry = self.registered_entry(task)?;
        let _run = entry.run.lock().await;
        let work = self.preparation_work(&entry).await?.invocation_window()?;
        self.resume_claim(task, &work).await
    }

    async fn resume_claim(
        &self,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<(Option<TaskWorkerAttempt>, NativeAllocation)> {
        self.child_admission(task, work).await?;
        let observed = recover_task(&self.runtime, task, work).await?;
        match observed.0 {
            WorkPull::Claimed(lease) => {
                let attempted = self
                    .runtime
                    .runtime
                    .resume_task_with_native_work(lease, observed.1, self, 8, work)
                    .await?;
                Ok((Some(attempted.0), attempted.1))
            }
            WorkPull::Unresolved { lease, error } => Ok((
                Some(TaskWorkerAttempt::Unresolved { lease, error }),
                observed.1,
            )),
            WorkPull::Idle => Ok((None, observed.1)),
        }
    }

    async fn execute_stock(
        &self,
        context: Arc<TaskContext>,
        fence: LeaseFence,
        stock: WorkflowCommand,
        plan: &TaskEffectPlan,
        bound: &BoundEffect,
        work: &NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        let maximum = context
            .scope()
            .limits()
            .file_bytes
            .min(self.protected.policy().maximum_payload_bytes);
        let (payload, _payload_custody) =
            crate::pi_root_provider::guarded_pi_value(plan, maximum, work)?;
        self.runtime
            .runtime
            .commands()
            .with_effects(bound.effects.clone())
            .execute_with_native_work(context, fence, stock, payload, work)?
            .await
    }

    pub(super) async fn effect_progress(
        &self,
        bound: &BoundEffect,
        mut observed: (EffectStatus, Option<NativeAllocation>),
    ) -> Result<TaskCommandProgress> {
        if !matches!(
            &observed.0,
            EffectStatus::Succeeded { .. } | EffectStatus::Failed { .. }
        ) {
            return Ok(TaskCommandProgress::Indeterminate);
        }
        let task = bound.owner.task_id();
        let entry = self.registered_entry(task)?;
        let work = self.preparation_work(&entry).await?.invocation_window()?;
        let maximum = bound
            .context
            .scope()
            .limits()
            .file_bytes
            .min(self.protected.policy().maximum_payload_bytes);
        let mut completed = match observed.0 {
            EffectStatus::Succeeded { result } => crate::pi_root_provider::guarded_pi_value(
                &Outcome::Succeeded(result),
                maximum,
                &work,
            )?,
            EffectStatus::Failed { message } => crate::pi_root_provider::guarded_pi_value(
                &Outcome::<Value>::Failed { message },
                maximum,
                &work,
            )?,
            _ => return Err(Error::Indeterminate(bound.command)),
        };
        if let Some(custody) = &mut observed.1 {
            completed.1.absorb(custody)?;
        }
        let (value, custody) = completed;
        Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody })
    }
}

pub(crate) async fn run_operation(
    runtime: &PiTaskRuntime,
    worker: &Worker,
    task: TaskId,
    commands: &dyn TaskCommandHost,
    work: &NativeMaterializationWork,
) -> Result<(Option<TaskWorkerAttempt>, NativeAllocation)> {
    let io = PreparationIo::from_native_work(work)?;
    let result = runtime
        .runtime
        .run_operation(worker, task, commands, 8, Some(&io))
        .await?;
    Ok((
        result,
        acyclic_harness::store::HistoryWork::new(io).into_custody()?,
    ))
}

impl TaskCommandHost for PiNativeCommandHost {
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            if command.kind == NATIVE_LAUNCH_TASK_COMMAND_KIND {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi native launch requires the SDK checked native command lane",
                });
            }
            self.runtime
                .runtime
                .commands()
                .execute(context, fence, command, payload)
                .await
        })
    }

    fn reconcile<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            if command.kind == NATIVE_LAUNCH_TASK_COMMAND_KIND {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi native reconciliation requires the SDK checked native command lane",
                });
            }
            self.runtime
                .runtime
                .commands()
                .reconcile(context, fence, command, payload)
                .await
        })
    }

    fn execute_with_native_work<'a>(
        &'a self,
        shared: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        work: &'a NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, TaskCommandProgress>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            if command.kind != NATIVE_LAUNCH_TASK_COMMAND_KIND {
                return self
                    .runtime
                    .runtime
                    .commands()
                    .execute_with_native_work(shared, fence, command, payload, work)?
                    .await;
            }
            let context = shared.as_ref();
            self.bind_invocation_work(
                context.durable_task_id().ok_or_else(|| {
                    unauthorized("Pi native invocation has no original durable owner")
                })?,
                work,
            )
            .await?;
            let (admitted, owner, intent) = self
                .authenticated_command(context, &fence, &command, payload, false, work)
                .await?;
            let input = &admitted.0;
            let recorded_effect = self
                .original_effect(context, input, command.operation_id, work)
                .await?;
            if let Some(effect) = recorded_effect.0.as_ref() {
                if let Some(bound) = self
                    .retained_planned(input.binding.task_id, command.operation_id, effect)
                    .await?
                {
                    let stock = bound
                        .staged
                        .as_ref()
                        .ok_or_else(|| unauthorized("Pi retained launch lost its original plan"))?;
                    let progress = self
                        .execute_stock(shared, fence, stock.clone(), &bound.plan.0, &bound, work)
                        .await?;
                    return Ok(progress);
                }
                let planned = effect.status == EffectStatus::Planned;
                let recovery = self
                    .bind_recovery(
                        Arc::clone(&shared),
                        input,
                        owner.clone(),
                        command.operation_id,
                        effect,
                        work,
                    )
                    .await?;
                if planned {
                    let bound = self
                        .prepare_effect(
                            (Arc::clone(&shared), input, owner),
                            &command,
                            intent,
                            true,
                            work,
                        )
                        .await?;
                    let stock = bound.staged.as_ref().ok_or_else(|| {
                        unauthorized("Pi restored launch has no original staged plan")
                    })?;
                    if bound.plan.0.provider != effect.provider
                        || bound.plan.0.guarantee != effect.guarantee
                        || bound.plan.0.effect_kind != effect.effect_kind
                        || &bound.plan.0.request != effect.request.as_ref()
                        || bound.plan.0.result_schema != effect.result_schema
                    {
                        return Err(unauthorized(
                            "Pi recovered Planned command changed its original effect plan",
                        ));
                    }
                    let progress = self
                        .execute_stock(shared, fence, stock.clone(), &bound.plan.0, &bound, work)
                        .await?;
                    return Ok(progress);
                }
                let progress = self
                    .effect_progress(
                        &recovery,
                        recovery
                            .effects
                            .run_task_effect(
                                &recovery.owner,
                                recovery.command,
                                &recovery.plan.0,
                                Some(recovery.plan.1.work()),
                                true,
                            )
                            .await?,
                    )
                    .await?;
                return Ok(progress);
            }
            let bound = self
                .prepare_effect(
                    (Arc::clone(&shared), input, owner),
                    &command,
                    intent,
                    false,
                    work,
                )
                .await?;
            let stock = bound
                .staged
                .as_ref()
                .ok_or_else(|| unauthorized("Pi launch has no original staged plan"))?;
            let progress = self
                .execute_stock(shared, fence, stock.clone(), &bound.plan.0, &bound, work)
                .await?;
            Ok(progress)
        })
    }

    fn reconcile_with_native_work<'a>(
        &'a self,
        shared: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        work: &'a NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, TaskCommandProgress>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            if command.kind != NATIVE_LAUNCH_TASK_COMMAND_KIND {
                return self
                    .runtime
                    .runtime
                    .commands()
                    .reconcile_with_native_work(shared, fence, command, payload, work)?
                    .await;
            }
            let context = shared.as_ref();
            self.bind_invocation_work(
                context.durable_task_id().ok_or_else(|| {
                    unauthorized("Pi native recovery has no original durable owner")
                })?,
                work,
            )
            .await?;
            let (admitted, owner, _) = self
                .authenticated_command(context, &fence, &command, payload, true, work)
                .await?;
            let input = &admitted.0;
            let recorded_effect = self
                .original_effect(context, input, command.operation_id, work)
                .await?;
            let Some(effect) = recorded_effect.0.as_ref() else {
                return Ok(TaskCommandProgress::Ready(Value::Null));
            };
            let bound = self
                .bind_recovery(shared, input, owner, command.operation_id, effect, work)
                .await?;
            let progress = self
                .effect_progress(
                    &bound,
                    bound
                        .effects
                        .run_task_effect(
                            &bound.owner,
                            bound.command,
                            &bound.plan.0,
                            Some(bound.plan.1.work()),
                            true,
                        )
                        .await?,
                )
                .await?;
            Ok(progress)
        })
    }
}
