//! Pi's native embedding over the existing fenced workflow driver.

use super::*;
use acyclic_harness::{
    conversation::FileRef,
    distributed::{WorkPull, Worker},
    filesystem::{FilesystemHost, TaskCommandHost, TaskCommandProgress, TaskWorkerAttempt},
    runtime::{DurableTaskHost, TaskContext},
    scheduler::{LeaseFence, ResourceSnapshot},
    workflow::{MachineCheckpoint, WorkflowCommand},
};
use acyclic_stream::{LocalStream};

type Runtime = pi_runtime::PiTaskRuntime<LocalStream>;
type Filesystem = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
type RootProvider = pi_root_provider::PiRootProvider<LocalStream>;

/// Borrowed embedding of canonical command adapters.
pub(crate) struct PiWorkflowHost<'a> {
    directory: &'a Path,
    runtime: Arc<Runtime>,
    filesystem: Arc<Filesystem>,
    stream: std::sync::Arc<LocalStream>,
    resources: &'a ServiceResources,
    retained: &'a AdapterState,
    root_provider: tokio::sync::Mutex<Option<Arc<RootProvider>>>,
    controls: Option<&'a pi_selected_host::PiControlHost<'a>>,
    work: acyclic_fs::NativeMaterializationWork,
}

impl<'a> PiWorkflowHost<'a> {
    pub(crate) fn new(
        directory: &'a Path,
        runtime: Arc<Runtime>,
        filesystem: Arc<Filesystem>,
        stream: std::sync::Arc<LocalStream>,
        resources: &'a ServiceResources,
        retained: &'a AdapterState,
        work: acyclic_fs::NativeMaterializationWork,
    ) -> Self {
        Self { directory, runtime, filesystem, stream, resources, retained,
            root_provider: tokio::sync::Mutex::new(None),
            controls: None,
            work,
        }
    }

    pub(crate) fn with_controls(mut self, controls: &'a pi_selected_host::PiControlHost<'a>) -> Self {
        self.controls = Some(controls);
        self
    }

    fn scoped_control_host(&self, kind: &str) -> Option<&pi_selected_host::PiControlHost<'_>> {
        match kind {
            "acyclic.pi.fork" | "acyclic.pi.observe-source" | "acyclic.pi.inspect" |
            "acyclic.pi.diff" | "acyclic.pi.import" | "acyclic.pi.writeback" |
            "acyclic.pi.operator-approval" | "acyclic.pi.builtin" => self.controls,
            _ => None,
        }
    }

    pub(crate) async fn drive_root(
        &self,
    ) -> acyclic_harness::Result<(Option<TaskWorkerAttempt>, acyclic_fs::NativeAllocation)> {
        let work = &self.work;
        let PluginTaskBinding::Pi(binding) = &self.retained.task_binding else {
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi driver requires its original admitted root",
            });
        };
        let admitted = pi_recovery::authenticate_pi_task(
            &self.runtime.runtime, binding,
            pi_recovery::PiTaskOrigin::RootState(self.retained), work,
        ).await?;
        drop(admitted.0);
        drop(admitted.1);
        let host = self.runtime.runtime.task_host();
        let recovered_custody;
        let recovered;
        (recovered, recovered_custody) =
            host.recover_work_with_native_work(binding.task_id, work).await?;
        match recovered {
            WorkPull::Claimed(lease) | WorkPull::Unresolved { lease, .. } => {
                let result = self.runtime.runtime
                    .resume_task_with_native_work(lease, recovered_custody, self, 8, work)
                    .await?;
                Ok((Some(result.0), result.1))
            }
            WorkPull::Idle => {
                drop(recovered_custody);
                let worker_custody;
                let worker;
                let (id, custody) = guarded_cli_text(
                    &["pi-native:", &self.resources.instance_id], work,
                )?;
                worker_custody = custody;
                worker = Worker {
                    id, available: ResourceSnapshot::default(), labels: BTreeMap::new(),
                };
                let result = self.runtime.runtime.run_operation_with_native_work(
                    &worker, binding.task_id, self, 8, work,
                ).await;
                drop(worker);
                drop(worker_custody);
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
    ) -> acyclic_harness::Result<Option<((FileRef, pi_root_allocation::PiRootAllocation), acyclic_fs::NativeAllocation)>> {
        let work = &self.work;
        let Some(provider) = self.root_provider.lock().await.clone() else { return Ok(None); };
        let owner = pi_owner::PiOwnerConfig::load_with_native_work(&self.resources.private_state_root, self.directory, work)?;
        self.runtime.verify_owner_with_native_work(&owner, work)?;
        let effect = acyclic_harness::effect_host::task_effect_id_with_native_work(
            owner.policy().root_task, pi_root_inputs::preparation_operation(owner.policy().root_task, work)?, work,
        )?;
        provider.observe_allocation_with_native_work(
            acyclic_harness::EffectAttemptId::from_bytes(effect.into_bytes()), work,
        ).await
    }

    /// Resume only the original passive root workflow.
    pub(crate) async fn control_with_native_work(
        &self, operation: acyclic_harness::OperationId, kind: &str, payload: &Value,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        let work = &self.work;
        let owner = pi_owner::PiOwnerConfig::load_with_native_work(&self.resources.private_state_root, self.directory, work)?;
        self.runtime.verify_owner_with_native_work(&owner, work)?;
        let payload_custody;
        let staged_payload;
        (staged_payload, payload_custody) = pi_root_provider::stage_pi_input(
            &owner, &self.filesystem, operation, "command", payload, work,
        ).await?;
        let kind_custody;
        let owned_kind;
        (owned_kind, kind_custody) = guarded_cli_text(&[kind], work)?;
        let command = WorkflowCommand { operation_id: operation, kind: owned_kind, payload: staged_payload };
        let result = self.control_command_with_native_work(command).await;
        drop(kind_custody);
        drop(payload_custody);
        result
    }

    pub(crate) async fn control_command_with_native_work(
        &self, command: WorkflowCommand,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        let operation = command.operation_id;
        self.control_step_with_native_work(command, false).await?
            .ok_or(acyclic_harness::Error::Indeterminate(operation))
    }

    fn checkpoint_control_reply(
        &self, mut checkpoint: (MachineCheckpoint, acyclic_fs::NativeAllocation),
        operation: acyclic_harness::OperationId,
    ) -> acyclic_harness::Result<Option<(Value, acyclic_fs::NativeAllocation)>> {
        let mut parsed = pi_task::control_reply(
            std::mem::take(&mut checkpoint.0.state), &self.work,
        )?;
        if let Err(cause) = checkpoint.1.absorb(&mut parsed.1) {
            drop(parsed.0);
            return Err(acyclic_harness::Error::NativeWork {
                cause, custody: Some(checkpoint.1),
            });
        }
        match parsed.0 {
            Some(reply) if reply.operation_id == operation => Ok(Some((reply.value, checkpoint.1))),
            _ => Ok(None),
        }
    }

    /// Advances only the retained command.
    pub(crate) async fn control_step_with_native_work(
        &self, command: WorkflowCommand, wake_pending: bool,
    ) -> acyclic_harness::Result<Option<(Value, acyclic_fs::NativeAllocation)>> {
        let owner = pi_owner::PiOwnerConfig::load_with_native_work(&self.resources.private_state_root, self.directory, &self.work)?;
        self.runtime.verify_owner_with_native_work(&owner, &self.work)?;
        let host = self.runtime.runtime.task_host();
        let task = owner.policy().root_task;
        let operation = command.operation_id;
        let input_custody;
        let input;
        (input, input_custody) = pi_root_provider::stage_pi_input(
            &owner, &self.filesystem, operation, "control",
            &pi_task::PiControlInput { command }, &self.work,
        ).await?;
        let checkpoint = self.runtime.runtime
            .observe_workflow_checkpoint_with_native_work(task, &self.work)
            .await?;
        if let Some(reply) = self.checkpoint_control_reply(checkpoint, operation)? {
            return Ok(Some(reply));
        }
        let suspension_custody;
        let suspension;
        (suspension, suspension_custody) = host
            .workflow_suspension_with_native_work(task, &self.work).await?;
        let Some(suspension) = suspension else {
            return Err(acyclic_harness::Error::Indeterminate(operation));
        };
        if let Some(waiting) = suspension.waiting_command {
            if waiting != operation {
                return Err(acyclic_harness::Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi Root is awaiting a different original command",
                });
            }
            if !wake_pending { return Ok(None); }
            host.wake_command_with_native_work(
                task, suspension.revision, waiting, &self.work,
            ).await?;
        } else {
            let mut task_text = [0; uuid::fmt::Hyphenated::LENGTH];
            let mut operation_text = [0; uuid::fmt::Hyphenated::LENGTH];
            let task_id = uuid::Uuid::from_bytes(task.into_bytes());
            let operation_id = uuid::Uuid::from_bytes(operation.into_bytes());
            let key_custody;
            let key;
            (key, key_custody) = guarded_cli_text(&[
                "acyclic.pi/control/",
                task_id.hyphenated().encode_lower(&mut task_text),
                "/",
                operation_id.hyphenated().encode_lower(&mut operation_text),
            ], &self.work)?;
            host.resume_workflow_with_native_work(
                task, suspension.revision, input,
                acyclic_harness::IdempotencyKey::new(key)?, &self.work,
            ).await?;
            drop(key_custody);
        }
        drop(suspension);
        drop(suspension_custody);
        self.drive_root().await?;
        let checkpoint = self.runtime.runtime
            .observe_workflow_checkpoint_with_native_work(task, &self.work)
            .await?;
        let reply = self.checkpoint_control_reply(checkpoint, operation)?;
        drop(input_custody);
        Ok(reply)
    }

    async fn root_control_context(
        &self, context: Arc<acyclic_harness::NativeTaskContext>, fence: &LeaseFence,
        protected: Arc<pi_owner::PiOwnerConfig>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Arc<acyclic_harness::NativeTaskContext>> {
        if context.context().durable_task_id() != Some(protected.policy().root_task) {
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi Root controls require their original Root context",
            });
        }
        let PluginTaskBinding::Pi(binding) = &self.retained.task_binding else {
            return Err(acyclic_harness::Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi root preparation requires a Pi state binding" });
        };
        let preparation = pi_root_inputs::PiRootPreparation::load(&self.resources.private_state_root, self.directory, &self.runtime, &self.filesystem, binding, pi_recovery::PiTaskOrigin::RootState(self.retained), Some(Arc::clone(&context)), work).await?;
        let owner = self.runtime.runtime.task_host().journal_owner_with_native_work(
            preparation.request.binding.task_id, fence, work).await?;
        owner.verify_with_native_work(false, work).await?;
        pi_root_provider::bind_root_output_context_with_native_work(
            context, &owner, protected, self.resources,
            self.filesystem.clone(), self.stream.clone(), &preparation.request.binding, &preparation.request.root, work,
        ).await
    }

    async fn root_effects(
        &self, context: Arc<acyclic_harness::NativeTaskContext>, fence: &LeaseFence,
        command: &WorkflowCommand, payload: &Value,
        owner: Arc<pi_owner::PiOwnerConfig>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Arc<acyclic_harness::effect_host::ConversationEffectHost<LocalStream>>> {
        if context.context().durable_task_id() != Some(owner.policy().root_task) {
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi root effect belongs to another task",
            });
        }
        let PluginTaskBinding::Pi(binding) = &self.retained.task_binding else {
            return Err(acyclic_harness::Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi root preparation requires a Pi state binding" });
        };
        let preparation = pi_root_inputs::PiRootPreparation::load(&self.resources.private_state_root, self.directory, &self.runtime, &self.filesystem, binding, pi_recovery::PiTaskOrigin::RootState(self.retained), Some(Arc::clone(&context)), work).await?;
        let journal_owner = self.runtime.runtime.task_host().journal_owner_with_native_work(
            preparation.request.binding.task_id, fence, work).await?;
        journal_owner.verify_with_native_work(false, work).await?;
        let expected = pi_root_provider::guarded_pi_value(
            &preparation.plan, MAXIMUM_CONTROL_MESSAGE_BYTES as u64, work,
        )?;
        let matches = &preparation.command == command && expected.0 == *payload;
        drop(expected.0);
        drop(expected.1);
        if !matches {
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi root command differs from original admission",
            });
        }
        let journal_owner = journal_owner.into_native_shared(work)?;
        let content = context.content_residency_verifier_with_native_work(
            Arc::clone(&journal_owner), work,
        )?;
        let effect_custody;
        let effect_authority;
        (effect_authority, effect_custody) = owner.effect_authority_with_native_work(work)?;
        let provider = pi_root_provider::root_provider(
            owner, self.resources, self.filesystem.clone(), self.stream.clone(),
            preparation, Some(journal_owner), work.clone(),
        )?;
        let effects = pi_owner::root_effects_with_native_work(
            self.stream.clone(), effect_authority, effect_custody,
            content, provider.clone(), work,
        )?;
        *self.root_provider.lock().await = Some(provider);
        Ok(effects)
    }

    async fn dispatch_native(
        &self, context: Arc<acyclic_harness::NativeTaskContext>, fence: LeaseFence,
        command: WorkflowCommand, payload: Value,
        work: &acyclic_fs::NativeMaterializationWork, reconcile: bool,
    ) -> acyclic_harness::Result<TaskCommandProgress> {
        let task = context.context().durable_task_id().ok_or(
            acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi command has no original task",
            },
        )?;
        let owner = pi_owner::PiOwnerConfig::load_readonly_with_native_work(
            &self.resources.private_state_root, self.directory, work)?
            .into_native_shared(work)?;
        self.runtime.verify_owner_with_native_work(&owner, work)?;
        if task == owner.policy().root_task
            && command.operation_id == pi_root_inputs::preparation_operation(task, work)?
        {
            let effects = self.root_effects(
                Arc::clone(&context), &fence, &command, &payload, owner, work,
            ).await?;
            let commands = self.runtime.runtime.commands().with_effects(effects);
            return if reconcile {
                commands.reconcile_with_native_work(context, fence, command, payload, work)?.await
            } else {
                commands.execute_with_native_work(context, fence, command, payload, work)?.await
            };
        }
        let context = if task == owner.policy().root_task {
            self.root_control_context(context, &fence, owner, work).await?
        } else {
            context
        };
        if let Some(controls) = self.scoped_control_host(&command.kind) {
            controls.run(context, fence, command, payload, reconcile, work).await
        } else if reconcile {
            self.runtime.runtime.commands()
                .reconcile_with_native_work(context, fence, command, payload, work)?.await
        } else {
            self.runtime.runtime.commands()
                .execute_with_native_work(context, fence, command, payload, work)?.await
        }
    }
}

impl TaskCommandHost for PiWorkflowHost<'_> {
    fn execute<'a>(
        &'a self, context: TaskContext, fence: LeaseFence,
        command: WorkflowCommand, payload: Value,
    ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness::Result<TaskCommandProgress>> {
        Box::pin(async move {
            self.runtime.runtime.commands().execute(context, fence, command, payload).await
        })
    }

    fn reconcile<'a>(
        &'a self, context: TaskContext, fence: LeaseFence,
        command: WorkflowCommand, payload: Value,
    ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness::Result<TaskCommandProgress>> {
        Box::pin(async move {
            self.runtime.runtime.commands().reconcile(context, fence, command, payload).await
        })
    }

    fn execute_with_native_work<'a>(
        &'a self, context: Arc<acyclic_harness::NativeTaskContext>, fence: LeaseFence,
        command: WorkflowCommand, payload: Value,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<acyclic_harness::NativeFutureBundle<'a, TaskCommandProgress>> {
        acyclic_harness::NativeFutureBundle::new(work, || {
            self.dispatch_native(context, fence, command, payload, work, false)
        })
    }

    fn reconcile_with_native_work<'a>(
        &'a self, context: Arc<acyclic_harness::NativeTaskContext>, fence: LeaseFence,
        command: WorkflowCommand, payload: Value,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<acyclic_harness::NativeFutureBundle<'a, TaskCommandProgress>> {
        acyclic_harness::NativeFutureBundle::new(work, || {
            self.dispatch_native(context, fence, command, payload, work, true)
        })
    }
}
