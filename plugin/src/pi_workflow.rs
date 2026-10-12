//! The Pi command host drives the existing fenced workflow.
use super::*;
use acyclic_harness::filesystem::PreparationIo;
use acyclic_harness::store::HistoryIo;
use acyclic_harness::store::native_work_io;
use acyclic_harness::{
    conversation::FileRef,
    distributed::{WorkPull, Worker},
    filesystem::{TaskCommandHost, TaskCommandProgress, TaskWorkerAttempt},
    runtime::{DurableTaskHost, TaskContext},
    scheduler::LeaseFence,
    workflow::{MachineCheckpoint, WorkflowCommand},
};
use pi_selected_host::PiControlHost;

impl PiControlHost<'_> {
    pub(crate) async fn drive_root(
        &self,
    ) -> acyclic_harness::Result<(Option<TaskWorkerAttempt>, acyclic_fs::NativeAllocation)> {
        let PluginTaskBinding::Pi(binding) = &self.retained.task_binding else {
            return Err(boundary("Pi driver requires its original admitted root"));
        };
        pi_recovery::authenticate_pi_task(
            &self.runtime.runtime,
            binding,
            pi_recovery::PiTaskOrigin::RootState(self.retained),
            &self.work,
        )
        .await?;
        let (recovered, custody) = {
            let mut io = native_work_io(&self.work)?;
            HistoryIo::retain_owned(
                &mut io,
                std::mem::size_of::<WorkPull>().max(std::mem::size_of::<TaskWorkerAttempt>())
                    as u64,
            )?;
            let value = self
                .runtime
                .runtime
                .task_host()
                .recover_work(binding.task_id, &mut Some(&mut io))
                .await?;
            (value, io.into_custody())
        };
        match recovered {
            WorkPull::Claimed(lease) | WorkPull::Unresolved { lease, .. } => {
                let result = self
                    .runtime
                    .runtime
                    .resume_task_with_native_work(lease, custody, self, 8, &self.work)
                    .await?;
                Ok((Some(result.0), result.1))
            }
            WorkPull::Idle => {
                drop(custody);
                let id =
                    guarded_cli_text(&["pi-native:", &self.resources.instance_id], &self.work)?;
                let worker = Worker {
                    id: id.0,
                    available: Default::default(),
                    labels: BTreeMap::new(),
                };
                let result = async {
                    let io = PreparationIo::from_native_work(&self.work)?;
                    let value = self
                        .runtime
                        .runtime
                        .run_operation(&worker, binding.task_id, self, 8, Some(&io))
                        .await?;
                    let PreparationIo::Native { custody, .. } = io else {
                        unreachable!()
                    };
                    Ok::<_, acyclic_harness::Error>((
                        value,
                        custody
                            .into_inner()
                            .unwrap_or_else(std::sync::PoisonError::into_inner),
                    ))
                }
                .await;
                drop(worker);
                drop(id.1);
                result
            }
        }
    }

    pub(crate) async fn created_roots(&self) -> BTreeMap<String, pi_root_provider::PiRootHandle> {
        match self.root_provider.lock().await.as_ref() {
            Some(provider) => provider.take_created_handles().await,
            None => BTreeMap::new(),
        }
    }

    pub(crate) async fn root_output(
        &self,
    ) -> acyclic_harness::Result<
        Option<(
            (FileRef, pi_root_allocation::PiRootAllocation),
            acyclic_fs::NativeAllocation,
        )>,
    > {
        let Some(provider) = self.root_provider.lock().await.clone() else {
            return Ok(None);
        };
        let owner = pi_owner::PiOwnerConfig::load(
            &self.resources.private_state_root,
            self.directory,
            &self.work,
        )?;
        self.runtime
            .verify_owner_with_native_work(&owner, &self.work)?;
        let task = owner.policy().root_task;
        let effect = acyclic_harness::effect_host::task_effect_id(
            task,
            pi_root_inputs::preparation_operation(task, &self.work)?,
            &mut Some(&mut native_work_io(&self.work)?),
        )?;
        provider
            .observe_allocation_with_native_work(
                acyclic_harness::EffectAttemptId::from_bytes(effect.into_bytes()),
                &self.work,
            )
            .await
    }

    pub(crate) async fn control_with_native_work(
        &self,
        operation: acyclic_harness::OperationId,
        kind: &str,
        payload: &Value,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        self.runtime
            .verify_owner_with_native_work(&self.protected, &self.work)?;
        let payload = pi_root_provider::stage_pi_input(
            &self.protected,
            &self.filesystem,
            operation,
            "command",
            payload,
            &self.work,
        )
        .await?;
        let kind = guarded_cli_text(&[kind], &self.work)?;
        let result = self
            .control_command_with_native_work(WorkflowCommand {
                operation_id: operation,
                kind: kind.0,
                payload: payload.0,
            })
            .await;
        drop(kind.1);
        drop(payload.1);
        result
    }

    pub(crate) async fn control_command_with_native_work(
        &self,
        command: WorkflowCommand,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        let operation = command.operation_id;
        self.control_step_with_native_work(command, false)
            .await?
            .ok_or(acyclic_harness::Error::Indeterminate(operation))
    }

    fn checkpoint_control_reply(
        &self,
        mut checkpoint: (MachineCheckpoint, acyclic_fs::NativeAllocation),
        operation: acyclic_harness::OperationId,
    ) -> acyclic_harness::Result<Option<(Value, acyclic_fs::NativeAllocation)>> {
        let mut reply =
            pi_task::control_reply(std::mem::take(&mut checkpoint.0.state), &self.work)?;
        if let Err(cause) = checkpoint.1.absorb(&mut reply.1) {
            drop(reply.0);
            return Err(acyclic_harness::Error::NativeWork {
                cause,
                custody: Some(checkpoint.1),
            });
        }
        Ok(reply
            .0
            .filter(|reply| reply.operation_id == operation)
            .map(|reply| (reply.value, checkpoint.1)))
    }

    pub(crate) async fn control_step_with_native_work(
        &self,
        command: WorkflowCommand,
        wake_pending: bool,
    ) -> acyclic_harness::Result<Option<(Value, acyclic_fs::NativeAllocation)>> {
        self.runtime
            .verify_owner_with_native_work(&self.protected, &self.work)?;
        let host = self.runtime.runtime.task_host();
        let task = self.protected.policy().root_task;
        let operation = command.operation_id;
        let input = pi_root_provider::stage_pi_input(
            &self.protected,
            &self.filesystem,
            operation,
            "control",
            &pi_task::PiControlInput { command },
            &self.work,
        )
        .await?;
        let checkpoint = self
            .runtime
            .runtime
            .observe_workflow_checkpoint_with_native_work(task, &self.work)
            .await?;
        if let Some(reply) = self.checkpoint_control_reply(checkpoint, operation)? {
            return Ok(Some(reply));
        }
        let suspension = host
            .workflow_suspension_with_native_work(task, &self.work)
            .await?;
        let Some(wait) = &suspension.0 else {
            return Err(acyclic_harness::Error::Indeterminate(operation));
        };
        if let Some(waiting) = wait.waiting_command {
            if waiting != operation {
                return Err(acyclic_harness::Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi Root is awaiting a different original command",
                });
            }
            if !wake_pending {
                return Ok(None);
            }
            host.wake_command_with_native_work(task, wait.revision, waiting, &self.work)
                .await?;
        } else {
            let key = self
                .work
                .display(&format_args!("acyclic.pi/control/{task}/{operation}"))?;
            host.resume_workflow(
                task,
                wait.revision,
                input.0,
                acyclic_harness::IdempotencyKey::new(key.0)?,
                Some(&self.work),
            )
            .await?;
            drop(key.1);
        }
        drop(suspension);
        self.drive_root().await?;
        let checkpoint = self
            .runtime
            .runtime
            .observe_workflow_checkpoint_with_native_work(task, &self.work)
            .await?;
        let result = self.checkpoint_control_reply(checkpoint, operation);
        drop(input.1);
        result
    }

    async fn dispatch_workflow(
        &self,
        context: Arc<acyclic_harness::runtime::NativeTaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        work: &acyclic_fs::NativeMaterializationWork,
        reconcile: bool,
    ) -> acyclic_harness::Result<TaskCommandProgress> {
        let task = context
            .context()
            .durable_task_id()
            .ok_or_else(|| boundary("Pi command has no original task"))?;
        let owner = pi_owner::PiOwnerConfig::load_readonly_with_native_work(
            &self.resources.private_state_root,
            self.directory,
            work,
        )?
        .into_native_shared(work)?;
        self.runtime.verify_owner_with_native_work(&owner, work)?;
        if task == owner.policy().root_task
            && command.operation_id == pi_root_inputs::preparation_operation(task, work)?
        {
            let PluginTaskBinding::Pi(binding) = &self.retained.task_binding else {
                return Err(boundary("Pi root preparation requires a Pi state binding"));
            };
            let preparation = pi_root_inputs::PiRootPreparation::load(
                &self.resources.private_state_root,
                self.directory,
                &self.runtime,
                &self.filesystem,
                binding,
                pi_recovery::PiTaskOrigin::RootState(self.retained),
                Some(context.clone()),
                work,
            )
            .await?;
            let journal_owner = self
                .runtime
                .runtime
                .task_host()
                .journal_owner_with_native_work(task, &fence, work)
                .await?;
            journal_owner
                .verify(false, &mut Some(&mut native_work_io(work)?))
                .await?;
            let expected = pi_root_provider::guarded_pi_value(
                &preparation.plan,
                MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
                work,
            )?;
            if preparation.command != command || expected.0 != payload {
                return Err(acyclic_harness::Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi root command differs from original admission",
                });
            }
            drop(expected);
            let journal_owner = journal_owner.into_native_shared(work)?;
            let content =
                context.content_residency_verifier_with_native_work(journal_owner.clone(), work)?;
            let authority = owner.effect_authority_with_native_work(work)?;
            let provider = pi_root_provider::root_provider(
                owner,
                self.resources,
                self.filesystem.clone(),
                self.stream.clone(),
                preparation,
                Some(journal_owner),
                work.clone(),
            )?;
            let effects = pi_owner::root_effects_with_native_work(
                self.stream.clone(),
                authority.0,
                authority.1,
                content,
                provider.clone(),
                work,
            )?;
            *self.root_provider.lock().await = Some(provider);
            let commands = self.runtime.runtime.commands().with_effects(effects);
            return if reconcile {
                commands
                    .reconcile_with_native_work(context, fence, command, payload, work)?
                    .await
            } else {
                commands
                    .execute_with_native_work(context, fence, command, payload, work)?
                    .await
            };
        }
        let context = if task == owner.policy().root_task {
            let PluginTaskBinding::Pi(binding) = &self.retained.task_binding else {
                return Err(boundary("Pi root preparation requires a Pi state binding"));
            };
            let preparation = pi_root_inputs::PiRootPreparation::load(
                &self.resources.private_state_root,
                self.directory,
                &self.runtime,
                &self.filesystem,
                binding,
                pi_recovery::PiTaskOrigin::RootState(self.retained),
                Some(context.clone()),
                work,
            )
            .await?;
            let journal_owner = self
                .runtime
                .runtime
                .task_host()
                .journal_owner_with_native_work(task, &fence, work)
                .await?;
            journal_owner
                .verify(false, &mut Some(&mut native_work_io(work)?))
                .await?;
            pi_root_provider::bind_root_output_context_with_native_work(
                context,
                &journal_owner,
                owner,
                self.resources,
                self.filesystem.clone(),
                self.stream.clone(),
                &preparation.request.binding,
                &preparation.request.root,
                work,
            )
            .await?
        } else {
            context
        };
        if matches!(
            command.kind.as_str(),
            "acyclic.pi.fork"
                | "acyclic.pi.observe-source"
                | "acyclic.pi.inspect"
                | "acyclic.pi.diff"
                | "acyclic.pi.import"
                | "acyclic.pi.writeback"
                | "acyclic.pi.operator-approval"
                | "acyclic.pi.builtin"
        ) {
            self.run(context, fence, command, payload, reconcile, work)
                .await
        } else if reconcile {
            self.runtime
                .runtime
                .commands()
                .reconcile_with_native_work(context, fence, command, payload, work)?
                .await
        } else {
            self.runtime
                .runtime
                .commands()
                .execute_with_native_work(context, fence, command, payload, work)?
                .await
        }
    }
}

impl TaskCommandHost for PiControlHost<'_> {
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness::Result<TaskCommandProgress>> {
        Box::pin(async move {
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
    ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness::Result<TaskCommandProgress>> {
        Box::pin(async move {
            self.runtime
                .runtime
                .commands()
                .reconcile(context, fence, command, payload)
                .await
        })
    }
    fn execute_with_native_work<'a>(
        &'a self,
        context: Arc<acyclic_harness::runtime::NativeTaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<acyclic_harness::NativeFutureBundle<'a, TaskCommandProgress>> {
        acyclic_harness::NativeFutureBundle::new(work, || {
            self.dispatch_workflow(context, fence, command, payload, work, false)
        })
    }
    fn reconcile_with_native_work<'a>(
        &'a self,
        context: Arc<acyclic_harness::runtime::NativeTaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<acyclic_harness::NativeFutureBundle<'a, TaskCommandProgress>> {
        acyclic_harness::NativeFutureBundle::new(work, || {
            self.dispatch_workflow(context, fence, command, payload, work, true)
        })
    }
}

fn boundary(reason: &'static str) -> acyclic_harness::Error {
    acyclic_harness::Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Unauthorized,
        reason,
    }
}
