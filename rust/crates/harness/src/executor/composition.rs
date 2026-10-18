use super::*;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Property: sampling removes all fresh tool grants, preserves every other
    // grant and leaves the exact manifest catalog/admitted revisions intact.
    proptest! {
        #[test]
        fn sampling_attenuation_keeps_the_pinned_catalog(grants in prop::collection::vec(
            prop_oneof!["tool:call:[a-z]{0,16}", "other:[a-z]{0,16}", Just("model:generate".into()), Just("tool:callback".into())], 0..16,
        )) {
            let mut tools = ToolRegistry::default();
            tools.register((crate::tool::files::write()?, None), &mut None, None)?;
            let before = tools.definitions(None)?;
            let expected = crate::Capabilities::new(grants.iter().filter(|grant| !grant.starts_with("tool:call:")).cloned());
            let executor = StockExecutor::new(
                Model::new("test".into(), "sampling".into(), "1".into(), Value::Null)?,
                Arc::new(crate::test_support::ScriptedModelProvider::new([])),
                ContextPipeline::default(), tools,
            ).with_tool_authority(RuntimeScope::new(crate::Capabilities::new(grants), Limits::default())?, None)?
                .without_tool_authority();
            prop_assert_eq!(executor.tool_scope.grants(), &expected);
            prop_assert_eq!(executor.tools.definitions(None)?, before);
            prop_assert!(stock_tool_definitions(&executor.tools, &executor.tool_scope, None)?.is_empty());
        }
    }
}

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
            summary_source: None,
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
            compaction: None,
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

    #[cfg(any(test, feature = "filesystem"))]
    pub(crate) fn with_summary_source(mut self, source: Option<Box<FileRef>>) -> Self {
        if source.is_some() {
            self.compaction = Some(crate::context::CompactionPolicy::Disabled);
        }
        self.summary_source = source;
        self
    }

    /// Replaces or disables the ordinary threshold policy without starting effects.
    #[must_use]
    pub fn with_compaction_policy(mut self, policy: crate::context::CompactionPolicy) -> Self {
        self.compaction = Some(policy);
        self
    }
    /// Appends one validated context stage to the existing ordered pipeline.
    pub fn with_context_stage(
        mut self,
        stage: Arc<dyn crate::context::ContextStage>,
    ) -> Result<Self> {
        if stage.name() == "context-manifest" && self.context.contains_stage(stage.name()) {
            return Err(Error::Conflict(
                "executor already has a context manifest".into(),
            ));
        }
        stage.validate(None)?;
        self.context = self.context.with(stage);
        Ok(self)
    }

    /// Binds request assembly and tool selection to one authenticated immutable manifest.
    pub async fn with_context_manifest(
        mut self,
        reference: FileRef,
        reader: Arc<dyn crate::conversation::ContentResidencyVerifier>,
    ) -> Result<Self> {
        if self.context.contains_stage("context-manifest") {
            return Err(Error::Conflict(
                "executor already has a context manifest".into(),
            ));
        }
        let (tools, stage) = crate::context::ContextManifest::bind(
            reference,
            reader.as_ref(),
            self.tools,
            self.limits,
        )
        .await?;
        self.tools = tools;
        self.with_context_stage(stage)
    }

    /// Removes fresh tool-call grants while preserving the pinned catalog and admitted versions.
    pub(crate) fn without_tool_authority(mut self) -> Self {
        self.tool_scope
            .grants
            .0
            .retain(|grant| !grant.starts_with("tool:call:"));
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
    #[cfg(feature = "filesystem")]
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
            validate_policy_identity(&policy.identity(), None)?;
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
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if context.native_work().is_ok() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native stock context requires its original work",
            });
        }
        let context = context.scoped(self.tool_scope.grants().clone(), self.limits)?;
        self.task_context = Some((Arc::new(context), execution_operation));
        StockExecution::ordinary(&self).verify_task_context_binding(execution_operation)?;
        Ok(self)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn with_shared_task_context(
        mut self,
        context: Arc<crate::runtime::TaskContext>,
        execution_operation: OperationId,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if context.native_work().is_ok() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native stock context requires its original work",
            });
        }
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
        context: &'a Arc<crate::runtime::TaskContext>,
        input: &'a TurnInput,
        journal: &'a dyn ExecutionJournal,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (TurnOutput, acyclic_fs::NativeAllocation)>> {
        native_execution_supported(self, context, journal, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let session = StockExecution {
                io: Some(&io),
                context: Some((context, input.operation_id)),
                ..StockExecution::ordinary(self)
            };
            native_result(
                &io,
                async {
                    session
                        .execute_progress(input, journal)
                        .await?
                        .into_output()
                },
                |output| {
                    native_size_add(
                        std::mem::size_of::<TurnOutput>() as u64,
                        native_output_payload_bytes(output)?,
                    )
                },
            )
            .await
        })
    }
}
