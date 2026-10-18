use super::*;
use crate::conversation::FileRef;

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
        tools: ToolRegistry,
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
            (model, provider),
            pipeline,
            tools,
            (
                Arc::clone(context),
                operation,
                self.host.clone(),
                task,
                fence,
            ),
            self.policy.clone(),
            self.interaction_owner
                .as_ref()
                .map(|_| journal.clone() as Arc<dyn crate::runtime::InteractionRouter>),
            (compaction, max_output_tokens),
        )
    }

    /// Constructs stock execution with the task-owned journal, selected tools and output volume.
    pub async fn stock_execution(
        &self,
        task: TaskId,
        fence: LeaseFence,
        turn: OperationId,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
        tools: ToolRegistry,
        output: Option<&VolumeRef>,
    ) -> Result<FilesystemTaskExecution<P, A, O>> {
        let task_context = Arc::new(self.task_context(task, fence.clone()).await?);
        self.stock_execution_with_context(
            task_context,
            fence,
            turn,
            model,
            provider,
            context,
            tools,
            output,
        )
        .await
    }

    pub(in crate::filesystem) async fn stock_execution_with_context(
        &self,
        task_context: Arc<TaskContext>,
        fence: LeaseFence,
        turn: OperationId,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
        tools: ToolRegistry,
        output: Option<&VolumeRef>,
    ) -> Result<FilesystemTaskExecution<P, A, O>> {
        let task = task_context.durable_task_id().ok_or_else(|| {
            Error::Unauthorized("stock model has no admitted task context".into())
        })?;
        if !task_context.original_task_binding_matches(task, &fence) {
            return Err(Error::Unauthorized(
                "stock model context differs from its worker lease".into(),
            ));
        }
        task_context.owned_task_binding(None)?;
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
        let task_context = match output {
            Some(volume) => task_context.with_output_volume(volume).await?,
            None => task_context,
        };
        let scope = task_context.scope().clone();
        let limits = scope.limits();
        let run_limits = scope.run_limits();
        let operation_id = execution_operation(task, turn, None)?;
        let task_files = match &self.task_files {
            Some(files) => match files.for_created_task(&task_context, &scope).await? {
                Some(created) => Some(Arc::new(created)),
                None => Some(files.clone()),
            },
            None => None,
        };
        let journal = self
            .open_execution_journal(task, fence.clone(), turn, &admission)
            .await?
            .with_task_files(task_files)
            .with_input_verifier(task_context.clone());
        let executor = StockExecutor::new(model, provider, context, tools);
        // Threshold compaction is the default only where summaries have a
        // private writer; enabling it elsewhere is rejected at turn entry.
        let executor = if task_context.private_summaries(None).is_ok() {
            executor
        } else {
            executor.with_compaction_policy(crate::context::CompactionPolicy::Disabled)
        };
        let executor = executor
            .with_limits(limits)
            .with_tool_authority(scope, self.policy.clone())?
            .with_durable_task(self.host.clone(), task, fence.clone())
            .with_shared_task_context(task_context, operation_id)?;
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

    pub(crate) fn with_summary_source(mut self, source: Option<Box<FileRef>>) -> Self {
        self.executor = self.executor.with_summary_source(source);
        self
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
            || self.executor.execute_progress(input, &self.journal),
            None,
            |_| false,
        )
        .await
    }

    /// Observes retained model/tool attempts without starting another step.
    pub fn reconcile(
        self,
        input: TurnInput,
        maximum_wait_millis: u32,
    ) -> impl std::future::Future<Output = Result<Option<TurnOutput>>> {
        let progress = if maximum_wait_millis == 0 {
            Err(Error::Invalid("invalid reconciliation allowance".into()))
        } else {
            Ok(self.reconcile_progress(input))
        };
        async move {
            use futures::future::{Either, select};
            use std::pin::pin;
            let progress = pin!(progress?);
            let timer = pin!(crate::live::sleep_millis(u64::from(maximum_wait_millis)));
            match select(timer, progress).await {
                Either::Left((elapsed, _)) => {
                    elapsed?;
                    Ok(None)
                }
                Either::Right((result, _)) => result?.into_output().map(Some),
            }
        }
    }

    pub(crate) fn reconcile_progress(
        mut self,
        input: TurnInput,
    ) -> impl std::future::Future<Output = Result<ExecutionProgress<TurnOutput>>> {
        let future = crate::conversation::BoundedContentFuture::new(None, || async move {
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
        });
        async move { future?.await }
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
