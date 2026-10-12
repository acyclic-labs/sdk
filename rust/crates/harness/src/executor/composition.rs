use super::*;

impl StockExecutor {
    /// Creates the stock loop without installing hidden stages or tools.
    #[must_use]
    pub fn new(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
        tools: ToolRegistry,
    ) -> Self {
        Self {
            model,
            max_output_tokens: None,
            provider,
            context,
            tools,
            limits: Limits::default(),
            tool_scope: RuntimeScope::default(),
            policy: None,
            policy_identity: None,
            task: None,
            inherited_prefix: None,
            task_context: None,
            compaction: crate::context::CompactionPolicy::default(),
            reconciliation_only: false,
        }
    }

    /// Sets the caller's output token budget for each model request.
    pub fn with_max_output_tokens(mut self, maximum: u32) -> Result<Self> {
        if maximum == 0 {
            return Err(Error::Invalid("model output token budget is zero".into()));
        }
        self.max_output_tokens = Some(maximum);
        Ok(self)
    }

    /// Replaces or disables the ordinary threshold policy without starting effects.
    #[must_use]
    pub fn with_compaction_policy(mut self, policy: crate::context::CompactionPolicy) -> Self {
        self.compaction = policy;
        self
    }
    /// Appends one validated context stage to the existing ordered pipeline.
    pub fn with_context_stage(
        mut self,
        stage: Arc<dyn crate::context::ContextStage>,
    ) -> Result<Self> {
        stage.validate(None)?;
        self.context = self.context.with(stage);
        Ok(self)
    }

    /// Replaces the selected tools without changing their admission machinery.
    #[must_use]
    pub fn with_tool_registry(mut self, tools: ToolRegistry) -> Self {
        self.tools = tools;
        self
    }
}

impl StockExecutor {
    /// Binds this worker's exact admitted task and execution fence.
    #[must_use]
    pub fn with_durable_task(
        mut self,
        host: Arc<dyn DurableTaskHost>,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> Self {
        self.task = Some((host, task_id, fence));
        self
    }

    /// Retains this exact composition while refusing every fresh effect intent.
    pub(crate) fn into_reconciliation(mut self) -> Self {
        self.reconciliation_only = true;
        self
    }
}

impl StockExecutor {
    /// Applies the composition's checked bounds to the stock loop.
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Binds a child to an immutable parent model prefix.
    pub fn with_inherited_prefix(
        mut self,
        prefix: FileRef,
        verifier: Arc<dyn crate::conversation::ContentResidencyVerifier>,
    ) -> Result<Self> {
        prefix.validate(&mut None)?;
        if prefix.descriptor().media_type() != crate::model::ModelPrefix::MEDIA_TYPE {
            return Err(Error::Invalid(
                "inherited model prefix has the wrong media type".into(),
            ));
        }
        self.inherited_prefix = Some((prefix, verifier));
        Ok(self)
    }
}

impl StockExecutor {
    /// Enforces the same explicit tool grants and policy in the stock model loop.
    pub fn with_tool_authority(
        mut self,
        scope: RuntimeScope,
        policy: Option<Arc<dyn ToolPolicy>>,
    ) -> Result<Self> {
        if let Some(policy) = &policy {
            validate_policy_identity(&policy.identity())?;
        }
        self.policy_identity = policy.as_ref().map(|policy| policy.identity());
        self.tool_scope = scope;
        self.policy = policy;
        Ok(self)
    }

    /// Carries the original admitted context into one bound stock execution.
    pub fn with_task_context(
        mut self,
        context: &crate::runtime::TaskContext,
        execution_operation: OperationId,
    ) -> Result<Self> {
        let context = context.scoped(self.tool_scope.grants().clone(), self.limits)?;
        self.task_context = Some((context, execution_operation));
        StockExecution::ordinary(&self).verify_task_context_binding(execution_operation)?;
        Ok(self)
    }
}

impl Executor for StockExecutor {
    fn execute_with_context<'a>(
        &'a self,
        context: &'a crate::runtime::TaskContext,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        Box::pin(async move {
            let bound = self
                .clone()
                .with_task_context(context, input.operation_id)?;
            bound.execute(input, journal).await
        })
    }

    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        Box::pin(async move { self.execute_progress(input, journal).await?.into_output() })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        StockExecutor::supports_native_execution(self, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a Arc<crate::runtime::NativeTaskContext>,
        input: &'a TurnInput,
        journal: &'a dyn ExecutionJournal,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (TurnOutput, acyclic_fs::NativeAllocation)>> {
        native_execution_supported(self, context, journal, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            io.absorb_native_custody(work.claim(
                std::mem::size_of::<TurnOutput>() as u64,
                0,
                acyclic_fs::WorkCounters::default(),
            )?)?;
            let output = {
                let session = native_session(self, context, input.operation_id, &io, None);
                session
                    .execute_progress(input, journal)
                    .await?
                    .into_output()?
            };
            let retained = native_size_add(
                std::mem::size_of::<TurnOutput>() as u64,
                native_output_payload_bytes(&output)?,
            )?;
            work.remaining()?;
            Ok((output, io.into_native_custody(retained)?))
        })
    }
}
