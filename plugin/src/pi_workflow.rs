//! Pi's native embedding over the existing fenced workflow driver.

use super::*;
use acyclic_harness::{
    TaskId,
    conversation::FileRef,
    distributed::{WorkPull, Worker},
    filesystem::{FilesystemHost, TaskCommandHost, TaskCommandProgress, TaskWorkerAttempt},
    runtime::{DurableTaskHost, TaskContext},
    scheduler::{LeaseFence, ResourceSnapshot},
    workflow::{MachineCheckpoint, WorkflowCommand},
};
use acyclic_stream::{LocalStream, StreamClient};

type Runtime = pi_runtime::PiTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;
type Filesystem = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
type RootProvider = pi_root_provider::PiRootProvider<LocalStream>;

/// Borrowed embedding of canonical command adapters. The provider instances and
/// live handles are transient; command identities and results belong exclusively
/// to the existing task, workflow and effect journals.
pub(crate) struct PiWorkflowHost<'a> {
    directory: &'a Path,
    runtime: Arc<Runtime>,
    filesystem: Arc<Filesystem>,
    stream: StreamClient<LocalStream>,
    resources: ServiceResources,
    retained: &'a AdapterState,
    root_provider: tokio::sync::Mutex<Option<Arc<RootProvider>>>,
    fork_controls: Option<&'a dyn TaskCommandHost>,
    selected_controls: Option<&'a dyn TaskCommandHost>,
    builtin_controls: Option<&'a dyn TaskCommandHost>,
    work: std::sync::OnceLock<acyclic_fs::NativeMaterializationWork>,
    invocation_cancellation: std::sync::OnceLock<CancellationToken>,
}

impl<'a> PiWorkflowHost<'a> {
    pub(crate) fn new(
        directory: &'a Path,
        runtime: Arc<Runtime>,
        filesystem: Arc<Filesystem>,
        stream: StreamClient<LocalStream>,
        resources: ServiceResources,
        retained: &'a AdapterState,
    ) -> Self {
        Self { directory, runtime, filesystem, stream, resources, retained,
            root_provider: tokio::sync::Mutex::new(None),
            fork_controls: None, selected_controls: None, builtin_controls: None,
            work: std::sync::OnceLock::new(),
            invocation_cancellation: std::sync::OnceLock::new() }
    }

    pub(crate) fn with_controls(
        mut self, fork: &'a dyn TaskCommandHost, selected: &'a dyn TaskCommandHost,
    ) -> Self {
        self.fork_controls = Some(fork);
        self.selected_controls = Some(selected);
        self
    }

    pub(crate) fn with_builtin_controls(mut self, builtin: &'a dyn TaskCommandHost) -> Self {
        self.builtin_controls = Some(builtin);
        self
    }

    pub(crate) fn with_work(mut self, work: acyclic_fs::NativeMaterializationWork) -> Self {
        self.invocation_cancellation = std::sync::OnceLock::from(work.cancellation().clone());
        self.work = std::sync::OnceLock::from(work);
        self
    }

    fn work(&self) -> acyclic_harness::Result<&acyclic_fs::NativeMaterializationWork> {
        self.work.get().ok_or(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unsupported,
            reason: "Pi requires its original native invocation ledger",
        })
    }

    fn accept<T>(
        &self, result: pi_root_provider::PiMeasuredResult<acyclic_fs::OperationReceipt<T>>,
    ) -> acyclic_harness::Result<T> {
        let work = self.work()?;
        match result {
            Ok(receipt) => {
                work.spent(receipt.work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
                Ok(receipt.value)
            }
            Err((error, prefix)) => {
                work.spent(prefix).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
                Err(error)
            }
        }
    }

    pub(crate) fn invocation_cancellation(&self) -> CancellationToken {
        self.invocation_cancellation.get_or_init(CancellationToken::new).clone()
    }

    fn scoped_control_host(&self, kind: &str) -> Option<&dyn TaskCommandHost> {
        match kind {
            "acyclic.pi.fork" | "acyclic.pi.observe-source" => self.fork_controls,
            "acyclic.pi.inspect" | "acyclic.pi.diff" | "acyclic.pi.import" |
            "acyclic.pi.writeback" | "acyclic.pi.operator-approval" => self.selected_controls,
            "acyclic.pi.builtin" => self.builtin_controls,
            _ => None,
        }
    }

    pub(crate) async fn drive_root(&self) -> acyclic_harness::Result<Option<TaskWorkerAttempt>> {
        let PluginTaskBinding::Pi(binding) = &self.retained.task_binding else {
            return Err(acyclic_harness::Error::Invalid("Pi driver requires its original admitted root".to_owned()));
        };
        // Recover the original reservation first. Pulling new work on an
        // uncertain retained lease would lose its effect/attempt ownership.
        pi_recovery::authenticate_pi_task(
            &self.runtime.runtime, binding,
            pi_recovery::PiTaskOrigin::Root(&pi_root::PiRootIntent::from_state(self.retained)
                .map_err(acyclic_harness::Error::Invalid)?),
        ).await?;
        let host = self.runtime.runtime.task_host();
        match host.recover_work(binding.task_id).await? {
            WorkPull::Claimed(lease) | WorkPull::Unresolved { lease, .. } =>
                Ok(Some(self.runtime.runtime.resume_task(lease, self, 8).await)),
            WorkPull::Idle => self.runtime.runtime.run_operation(&Worker {
                id: format!("pi-native:{}", self.resources.instance_id),
                available: ResourceSnapshot::default(), labels: BTreeMap::new(),
            }, binding.task_id, self, 8).await,
        }
    }

    pub(crate) async fn created_roots(&self) -> BTreeMap<String, pi_root_provider::PiRootHandle> {
        match self.root_provider.lock().await.as_ref() {
            Some(provider) => provider.take_created_handles().await,
            None => BTreeMap::new(),
        }
    }

    pub(crate) async fn root_output(&self) -> acyclic_harness::Result<Option<(FileRef, pi_root_allocation::PiRootAllocation)>> {
        let Some(provider) = self.root_provider.lock().await.clone() else { return Ok(None); };
        let owner = pi_owner::PiOwnerConfig::load(self.directory)?;
        self.runtime.verify_owner(&owner)?;
        let effect = acyclic_harness::effect_host::task_effect_id(
            owner.policy().root_task, pi_root_inputs::preparation_operation(owner.policy().root_task),
        )?;
        provider.observe_allocation(acyclic_harness::EffectAttemptId::from_bytes(effect.into_bytes()))
            .await
    }

    /// Resume only the original passive root workflow. Its checkpoint is the
    /// acknowledgement: no RPC retry ledger or replacement admission is needed.
    pub(crate) async fn control_with_native_work(
        &self, operation: acyclic_harness::OperationId, kind: &str, payload: &Value,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        let owner = pi_owner::PiOwnerConfig::load(self.directory)?;
        self.runtime.verify_owner(&owner)?;
        let payload_custody;
        let payload;
        (payload, payload_custody) = pi_root_provider::stage_pi_input_with_native_work(
            &owner, &self.filesystem, operation, "command", payload, self.work()?,
        ).await?;
        let command = WorkflowCommand {
            operation_id: operation, kind: kind.to_owned(), payload,
        };
        let result = self.control_command_with_native_work(command).await;
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
        let mut parsed = pi_task::control_reply_owned(
            std::mem::take(&mut checkpoint.0.state), self.work()?,
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

    /// Advances only the retained command. `wake_pending` is supplied by the
    /// trusted actor after its original admitted preparation is durably saved.
    pub(crate) async fn control_step_with_native_work(
        &self, command: WorkflowCommand, wake_pending: bool,
    ) -> acyclic_harness::Result<Option<(Value, acyclic_fs::NativeAllocation)>> {
        let owner = pi_owner::PiOwnerConfig::load(self.directory)?;
        self.runtime.verify_owner(&owner)?;
        let host = self.runtime.runtime.task_host();
        let task = owner.policy().root_task;
        let operation = command.operation_id;
        let input_custody;
        let input;
        (input, input_custody) = pi_root_provider::stage_pi_input_with_native_work(
            &owner, &self.filesystem, operation, "control",
            &pi_task::PiControlInput { command }, self.work()?,
        ).await?;
        let checkpoint = self.runtime.runtime
            .observe_workflow_checkpoint_with_native_work(task, self.work()?)
            .await?;
        if let Some(reply) = self.checkpoint_control_reply(checkpoint, operation)? {
            return Ok(Some(reply));
        }
        let Some(suspension) = host.workflow_suspension(task).await? else {
            return Err(acyclic_harness::Error::Indeterminate(operation));
        };
        if let Some(waiting) = suspension.waiting_command {
            if waiting != operation {
                return Err(acyclic_harness::Error::Conflict("Pi Root is awaiting a different original command".to_owned()));
            }
            if !wake_pending { return Ok(None); }
            host.wake_command(task, suspension.revision, waiting).await?;
        } else {
            host.resume_workflow(task, suspension.revision, input,
                acyclic_harness::IdempotencyKey::new(format!("acyclic.pi/control/{task}/{operation}"))
                    ?,
            ).await?;
        }
        self.drive_root().await?;
        let checkpoint = self.runtime.runtime
            .observe_workflow_checkpoint_with_native_work(task, self.work()?)
            .await?;
        let reply = self.checkpoint_control_reply(checkpoint, operation)?;
        drop(input_custody);
        Ok(reply)
    }

    async fn root_control_context(
        &self, context: TaskContext, fence: &LeaseFence,
        protected: pi_owner::PiOwnerConfig,
    ) -> acyclic_harness::Result<TaskContext> {
        let task = protected.policy().root_task;
        if context.durable_task_id() != Some(task) {
            return Err(acyclic_harness::Error::Unauthorized("Pi Root controls require their original Root context".to_owned()));
        }
        let host = self.runtime.runtime.task_host();
        let admission = host.observe_admission(task).await?;
        let input: pi_task::PiTaskInput = serde_json::from_value(admission.input)
            .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
        let pi_task::PiTaskExecution::Host { root, .. } = input.execution else {
            return Err(acyclic_harness::Error::Unauthorized("Pi Root output binding has no original host admission".to_owned()));
        };
        let original = pi_root_inputs::PiRootRequest { binding: input.binding, root };
        let owner = host.journal_owner(task, fence.clone()).await?;
        self.accept(pi_root_provider::bind_root_output_context(
            context, &owner, protected, self.resources.clone(), self.filesystem.clone(),
            self.stream.clone(), &original, self.work()?
                .remaining().map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?,
            self.invocation_cancellation.get_or_init(CancellationToken::new),
        ).await)
    }

    async fn root_effects(
        &self, task: TaskId, fence: LeaseFence,
        command: &WorkflowCommand, payload: &Value,
    ) -> acyclic_harness::Result<Arc<acyclic_harness::effect_host::ConversationEffectHost<LocalStream>>> {
        let owner = pi_owner::PiOwnerConfig::load(self.directory)?;
        self.runtime.verify_owner(&owner)?;
        let PluginTaskBinding::Pi(binding) = &self.retained.task_binding else {
            return Err(acyclic_harness::Error::Unauthorized("Pi root binding missing".to_owned()));
        };
        if task != binding.task_id || task != owner.policy().root_task {
            return Err(acyclic_harness::Error::Unauthorized("Pi root effect belongs to another task".to_owned()));
        }
        let request = pi_root_inputs::PiRootRequest {
            binding: binding.as_ref().clone(),
            root: pi_root::PiRootIntent::from_state(self.retained)
                .map_err(acyclic_harness::Error::Unauthorized)?,
        };
        let preparation = pi_root_inputs::PiRootPreparation::load(
            self.directory, &self.runtime, &self.filesystem, &request, self.work()?,
        ).await?;
        if preparation.command() != command || serde_json::to_value(preparation.plan())
            .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))? != *payload
        {
            return Err(acyclic_harness::Error::Conflict("Pi root command differs from original admission".to_owned()));
        }
        let journal_owner = self.runtime.runtime.task_host().journal_owner(task, fence).await?;
        let provider = pi_root_provider::root_provider(
            owner.clone(), self.resources.clone(), self.filesystem.clone(), self.stream.clone(),
            preparation, journal_owner, self.work()?
                .remaining().map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?,
            self.invocation_cancellation(),
        )?;
        let effects = Arc::new(owner.root_effects(
            self.stream.clone(), self.filesystem.clone(), provider.clone(),
        )?);
        *self.root_provider.lock().await = Some(provider);
        Ok(effects)
    }
}

impl TaskCommandHost for PiWorkflowHost<'_> {
    fn execute<'a>(
        &'a self, context: TaskContext, fence: LeaseFence,
        command: WorkflowCommand, payload: Value,
    ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness::Result<TaskCommandProgress>> {
        Box::pin(async move {
            let task = context.durable_task_id().ok_or_else(||
                acyclic_harness::Error::Unauthorized("Pi command has no original task".to_owned()))?;
            let owner = pi_owner::PiOwnerConfig::load(self.directory)?;
            self.runtime.verify_owner(&owner)?;
            if task == owner.policy().root_task && command.operation_id ==
                pi_root_inputs::preparation_operation(task)
            {
                let effects = self.root_effects(task, fence.clone(), &command, &payload).await?;
                self.runtime.runtime.commands().with_effects(effects)
                    .execute(context, fence, command, payload).await
            } else if let Some(controls) = self.scoped_control_host(&command.kind) {
                controls.execute(context, fence, command, payload).await
            } else {
                let context = self.root_control_context(context, &fence, owner).await?;
                self.runtime.runtime.commands().execute(context, fence, command, payload).await
            }
        })
    }

    fn reconcile<'a>(
        &'a self, context: TaskContext, fence: LeaseFence,
        command: WorkflowCommand, payload: Value,
    ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness::Result<TaskCommandProgress>> {
        Box::pin(async move {
            let task = context.durable_task_id().ok_or_else(||
                acyclic_harness::Error::Unauthorized("Pi reconciliation has no original task".to_owned()))?;
            let owner = pi_owner::PiOwnerConfig::load(self.directory)?;
            self.runtime.verify_owner(&owner)?;
            if task == owner.policy().root_task &&
                command.operation_id == pi_root_inputs::preparation_operation(task)
            {
                let effects = self.root_effects(task, fence.clone(), &command, &payload).await?;
                self.runtime.runtime.commands().with_effects(effects)
                    .reconcile(context, fence, command, payload).await
            } else if let Some(controls) = self.scoped_control_host(&command.kind) {
                controls.reconcile(context, fence, command, payload).await
            } else {
                let context = self.root_control_context(context, &fence, owner).await?;
                self.runtime.runtime.commands().reconcile(context, fence, command, payload).await
            }
        })
    }
}
