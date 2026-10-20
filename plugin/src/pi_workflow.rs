//! The Pi command host drives the existing fenced workflow.
use super::*;
use acyclic_harness::filesystem::PreparationIo;
use acyclic_harness::{
    conversation::FileRef,
    distributed::{WorkPull, Worker},
    filesystem::{TaskCommandHost, TaskCommandProgress, TaskWorkerAttempt},
    runtime::{TaskContext, TaskStateProvider},
    scheduler::LeaseFence,
    workflow::{MachineTransition, WorkflowCommand},
};
use pi_host::PiControlHost;

impl PiControlHost<'_> {
    pub(crate) async fn drive_root(
        &self,
    ) -> acyclic_harness::Result<(Option<TaskWorkerAttempt>, acyclic_fs::NativeAllocation)> {
        let PluginTaskBinding::Pi(binding) = &self.retained.task_binding else {
            return Err(boundary("Pi driver requires its original admitted root"));
        };
        {
            let admission = self
                .runtime
                .runtime
                .task_host()
                .observe_admission_with_native_work(binding.task_id, &self.work)?
                .await?;
            pi_recovery::authenticate_pi_task(
                &admission.0,
                binding,
                pi_recovery::PiTaskOrigin::RootState(self.retained),
                &self.work,
                self.runtime.definition.identity(),
            )?;
        }
        let (recovered, custody) =
            pi_native_host::recover_task(&self.runtime, binding.task_id, &self.work).await?;
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
                let result = pi_native_host::run_operation(
                    &self.runtime,
                    &worker,
                    binding.task_id,
                    self,
                    &self.work,
                )
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
            Arc<pi_root_provider::PiRootProvider>,
            (
                (FileRef, pi_root_allocation::PiRootAllocation),
                acyclic_fs::NativeAllocation,
            ),
        )>,
    > {
        let Some(provider) = self.root_provider.lock().await.clone() else {
            return Ok(None);
        };
        let owner =
            pi_owner::PiOwnerConfig::load(&self.resources.data, self.directory, &self.work)?;
        self.runtime
            .verify_owner_with_native_work(&owner, &self.work)?;
        let task = owner.policy().root_task;
        let effect = acyclic_harness::effect_host::task_effect_id(
            task,
            pi_root_inputs::preparation_operation(task, &self.work)?,
            &mut Some(&mut &PreparationIo::from_native_work(&self.work)?),
        )?;
        let allocation = provider
            .observe_allocation_with_native_work(
                acyclic_harness::EffectAttemptId::from_bytes(effect.into_bytes()),
                &self.work,
            )
            .await?;
        Ok(allocation.map(|allocation| (provider, allocation)))
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
        mut checkpoint: (Option<MachineTransition>, acyclic_fs::NativeAllocation),
        operation: acyclic_harness::OperationId,
    ) -> acyclic_harness::Result<Option<(Value, acyclic_fs::NativeAllocation)>> {
        let Some(transition) = checkpoint.0.take() else {
            return Ok(None);
        };
        let mut reply = pi_task::control_reply(transition.state, &self.work)?;
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
        let workflow = self.runtime.workflow(task, &self.work).await?;
        let revision = workflow.0.checkpoint().revision;
        let checkpoint = workflow
            .0
            .latest_transition_with_native_work(&self.work)
            .await?;
        let waiting = match checkpoint
            .0
            .as_ref()
            .map(|transition| transition.commands.as_slice())
        {
            Some([command]) => Some(command.operation_id),
            Some([]) => None,
            _ => return Err(acyclic_harness::Error::Indeterminate(operation)),
        };
        drop(workflow);
        if let Some(reply) = self.checkpoint_control_reply(checkpoint, operation)? {
            return Ok(Some(reply));
        }
        if let Some(waiting) = waiting {
            if waiting != operation {
                return Err(acyclic_harness::Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi Root is awaiting a different original command",
                });
            }
            if !wake_pending {
                return Ok(None);
            }
            let wake = pi_root_provider::stage_pi_input(
                &self.protected,
                &self.filesystem,
                operation,
                "command-wake",
                &(),
                &self.work,
            )
            .await?;
            let key = self.work.display(&format_args!(
                "task-command-wake:{task}:{revision}:{waiting}"
            ))?;
            host.resume_workflow(
                task,
                revision,
                wake.0,
                acyclic_harness::IdempotencyKey::new(key.0)?,
                Some(waiting),
                &mut Some(&mut &PreparationIo::from_native_work(&self.work)?),
            )
            .await?;
            drop(wake.1);
            drop(key.1);
        } else {
            let key = self
                .work
                .display(&format_args!("acyclic.pi/control/{task}/{operation}"))?;
            host.resume_workflow(
                task,
                revision,
                input.0,
                acyclic_harness::IdempotencyKey::new(key.0)?,
                None,
                &mut Some(&mut &PreparationIo::from_native_work(&self.work)?),
            )
            .await?;
            drop(key.1);
        }
        self.drive_root().await?;
        let workflow = self.runtime.workflow(task, &self.work).await?;
        let checkpoint = workflow
            .0
            .latest_transition_with_native_work(&self.work)
            .await?;
        let result = self.checkpoint_control_reply(checkpoint, operation);
        drop(input.1);
        result
    }

    async fn dispatch_workflow(
        &self,
        context: Arc<acyclic_harness::runtime::TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        work: &acyclic_fs::NativeMaterializationWork,
        reconcile: bool,
    ) -> acyclic_harness::Result<TaskCommandProgress> {
        let task = context
            .durable_task_id()
            .ok_or_else(|| boundary("Pi command has no original task"))?;
        let owner = pi_owner::PiOwnerConfig::load(&self.resources.data, self.directory, work)?
            .into_native_shared(work)?;
        self.runtime.verify_owner_with_native_work(&owner, work)?;
        let root = if task == owner.policy().root_task {
            let PluginTaskBinding::Pi(binding) = &self.retained.task_binding else {
                return Err(boundary("Pi root preparation requires a Pi state binding"));
            };
            let preparation = pi_root_inputs::PiRootPreparation::load(
                &owner,
                &self.runtime,
                &self.filesystem,
                binding,
                pi_recovery::PiTaskOrigin::RootState(self.retained),
                Some(context.clone()),
                work,
            )
            .await?;
            let journal_owner =
                context.journal_owner::<acyclic_stream::LocalStream>(&fence, work)?;
            journal_owner
                .verify(
                    false,
                    &mut Some(&mut &PreparationIo::from_native_work(work)?),
                )
                .await?;
            Some((preparation, journal_owner))
        } else {
            None
        };
        let context = if let Some((preparation, journal_owner)) = root {
            if command.operation_id == pi_root_inputs::preparation_operation(task, work)? {
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
                let content = acyclic_harness::runtime::content_residency_verifier(
                    Arc::clone(&context),
                    Arc::clone(&journal_owner),
                    Some(&PreparationIo::from_native_work(work)?),
                )?;
                let authority = owner.effect_authority_with_native_work(work)?;
                let provider = pi_root_provider::root_provider(
                    owner,
                    self.resources,
                    self.filesystem.clone(),
                    self.stream.clone(),
                    preparation,
                    Some(&fence),
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
            pi_root_provider::bind_root_output_context(
                &journal_owner,
                owner,
                self.resources,
                self.filesystem.clone(),
                self.stream.clone(),
                preparation,
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
        context: Arc<acyclic_harness::runtime::TaskContext>,
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
        context: Arc<acyclic_harness::runtime::TaskContext>,
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
