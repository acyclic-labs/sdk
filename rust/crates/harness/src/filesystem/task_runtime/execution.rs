use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) fn stock_executor_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        operation: OperationId,
        context: &Arc<crate::runtime::TaskContext>,
        model: &Model,
        provider: Arc<dyn ModelProvider>,
        pipeline: &ContextPipeline,
        journal: &Arc<FilesystemExecutionJournal<P, A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
        compaction: &crate::context::CompactionPolicy,
        max_output_tokens: Option<u32>,
    ) -> Result<crate::executor::NativeStockExecutor> {
        if !context.original_task_binding_matches(task, fence)
            || !context.native_metadata_matches(work)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "stock model differs from original task binding",
            });
        }
        crate::executor::NativeStockExecutor::new(
            (model,
            provider),
            pipeline,
            &self.tools,
            (Arc::clone(context),
            operation,
            self.host.clone(),
            task,
            fence),
            self.policy.clone(),
            self.interaction_owner
                .as_ref()
                .map(|_| journal.clone() as Arc<dyn crate::runtime::InteractionRouter>),
            (compaction,
            max_output_tokens),
        )
    }

    /// Constructs stock execution with the task-owned journal and optional output volume.
    pub async fn stock_execution(
        &self,
        task: TaskId,
        fence: LeaseFence,
        turn: OperationId,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
        output: Option<&VolumeRef>,
    ) -> Result<FilesystemTaskExecution<P, A, O>> {
        let admission = self.host.observe_admission(task).await?;
        if !admission.grants.contains("model:generate") {
            return Err(Error::Unauthorized(
                "task scope lacks model:generate".into(),
            ));
        }
        if admission.execution.is_some() {
            return Err(Error::Unsupported(
                "persistent local stock composition has no routed execution provider".into(),
            ));
        }
        let task_context = Arc::new(self.task_context(task, fence.clone()).await?);
        let task_context = match output {
            Some(volume) => task_context.with_output_volume(volume).await?,
            None => task_context,
        };
        let scope = task_context.scope().clone();
        let limits = scope.limits();
        let run_limits = scope.run_limits();
        let operation_id = execution_operation(task, turn, None)?;
        let journal = self
            .open_execution_journal(task, fence.clone(), turn, &admission)
            .await?;
        let executor = StockExecutor::new(model, provider, context, self.tools.clone())
            .with_limits(limits)
            .with_tool_authority(scope, self.policy.clone())?
            .with_durable_task(self.host.clone(), task, fence.clone())
            .with_task_context(&task_context, operation_id)?;
        Ok(FilesystemTaskExecution {
            operation_id,
            executor,
            journal,
            run_limits,
            host: self.host.clone(),
            task,
            fence,
        })
    }
}

/// A stock executor and its mandatory task-owned journal.
pub struct FilesystemTaskExecution<P, A, O> {
    pub(super) operation_id: OperationId,
    pub(super) executor: StockExecutor,
    pub(super) journal: FilesystemExecutionJournal<P, A, O>,
    pub(super) run_limits: TaskRunLimits,
    pub(super) host: Arc<CoordinatorTaskHost<P>>,
    pub(super) task: TaskId,
    pub(super) fence: LeaseFence,
}

impl<P, A, O> FilesystemTaskExecution<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Stable task-namespaced turn identity to use in `TurnInput` after reopen.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Replaces or disables compaction while retaining task accounting and fencing.
    #[must_use]
    pub fn with_compaction_policy(mut self, policy: crate::context::CompactionPolicy) -> Self {
        self.executor = self.executor.with_compaction_policy(policy);
        self
    }

    /// Sets the output token budget without replacing task ownership or its journal.
    pub fn with_max_output_tokens(mut self, maximum: u32) -> Result<Self> {
        self.executor = self.executor.with_max_output_tokens(maximum)?;
        Ok(self)
    }

    /// Executes or reconciles using the bound journal and existing stock loop.
    pub async fn execute(&self, input: TurnInput) -> Result<TurnOutput> {
        self.execute_progress(input).await?.into_output()
    }

    pub(crate) async fn execute_progress(
        &self,
        input: TurnInput,
    ) -> Result<ExecutionProgress<TurnOutput>> {
        self.validate_input(&input)?;
        deadline::run(
            &self.host,
            self.task,
            Some(&self.fence),
            self.run_limits.deadline_epoch_ms,
            self.operation_id,
            self.executor.execute_progress(input, &self.journal),
            None,
            |_| false,
        )
        .await
    }

    /// Observes retained model/tool attempts without starting another step.
    pub async fn reconcile(
        self,
        input: TurnInput,
        maximum_wait_millis: u32,
    ) -> Result<Option<TurnOutput>> {
        if maximum_wait_millis == 0 {
            return Err(Error::Invalid("invalid reconciliation allowance".into()));
        }
        deadline::observe(maximum_wait_millis, async {
            self.reconcile_progress(input).await?.into_output()
        })
        .await
    }

    pub(crate) async fn reconcile_progress(
        mut self,
        input: TurnInput,
    ) -> Result<ExecutionProgress<TurnOutput>> {
        self.validate_input(&input)?;
        self.host
            .verify_execution_owner(self.task, &self.fence, None)?
            .await?;
        if self
            .run_limits
            .deadline_epoch_ms
            .is_some_and(|deadline| self.host.now_unix_millis() >= deadline)
        {
            self.host
                .expire_task_deadline(self.task, Some(&self.fence), &mut None)
                .await?;
        }
        self.executor = self.executor.into_reconciliation();
        self.executor.execute_progress(input, &self.journal).await
    }

    pub(super) fn validate_input(&self, input: &TurnInput) -> Result<()> {
        if input.operation_id != self.operation_id {
            return Err(Error::Unauthorized(
                "turn belongs to another task execution".into(),
            ));
        }
        if self
            .run_limits
            .max_steps
            .is_some_and(|bound| u64::from(input.max_steps) > bound as u64)
        {
            return Err(Error::Unauthorized(
                "turn exceeds admitted task step ceiling".into(),
            ));
        }
        Ok(())
    }
}
