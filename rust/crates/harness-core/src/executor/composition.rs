use super::*;

impl StockExecutor {
    #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn with_hooks(mut self, hooks: Option<Arc<crate::hooks::BoundHooks>>) -> Self {
        self.hooks = hooks.filter(|hooks| hooks.rules.has_execution_hooks());
        self
    }

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
            #[cfg(feature = "filesystem")]
            profile: None,
            #[cfg(all(feature = "code-mode", not(target_arch = "wasm32")))]
            code_runtime: None,
            max_output_tokens: None,
            provider,
            context,
            tools,
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            hooks: None,
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            prompt_block: None,
            limits: Limits::default(),
            tool_scope: RuntimeScope::default(),
            policy: None,
            policy_identity: None,
            inherited_prefix: None,
            task_context: None,
            compaction: None,
            reconciliation_only: false,
            live_reply: None,
        }
    }

    /// Binds the actor's immutable profile to this original executor. Role
    /// availability is checked in the same dispatch policy as approvals.
    #[cfg(feature = "filesystem")]
    pub(crate) fn with_profile(
        mut self,
        profiles: Arc<crate::profile::Profiles>,
        pin: crate::agent::AgentProfilePin,
    ) -> Result<Self> {
        let key = format!("{}/{}", self.model.provider, self.model.name);
        let resolved = profiles.for_model(&pin, &key)?;
        self.max_output_tokens = resolved.overlay().max_output_tokens;
        self.profile = Some((profiles, pin, key));
        Ok(self)
    }

    /// Stages each model dispatch's text while it streams; see [`LiveReply`].
    #[must_use]
    pub fn with_live_reply(mut self, reply: LiveReply) -> Self {
        self.live_reply = Some(reply);
        self
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
    /// Retains this exact composition while refusing every fresh effect intent.
    #[cfg(test)]
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
    /// Enforces the same explicit tool grants and approval policy in the stock
    /// model loop. Every tool authority has a policy: one accepting every
    /// action is an explicit choice, never the result of a missing one.
    pub fn with_tool_authority(
        mut self,
        scope: RuntimeScope,
        policy: Arc<dyn ToolPolicy>,
    ) -> Result<Self> {
        validate_policy_identity(&policy.identity(), None)?;
        self.policy_identity = Some(policy.identity());
        self.tool_scope = scope;
        self.policy = Some(policy);
        Ok(self)
    }

    /// Carries the original admitted context into one bound stock execution.
    pub fn with_task_context(
        mut self,
        context: &crate::runtime::TaskContext,
        execution_operation: OperationId,
    ) -> Result<Self> {
        let context = context.scoped(self.tool_scope.grants().clone(), self.limits)?;
        self.task_context = Some((Arc::new(context), execution_operation));
        StockExecution::ordinary(&self).verify_task_context_binding(execution_operation)?;
        Ok(self)
    }
}

impl Executor for StockExecutor {
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        Box::pin(async move {
            StockExecutor::execute_progress(self, input, journal)
                .await?
                .into_output()
        })
    }

    fn execute_progress<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<ExecutionProgress<TurnOutput>>> {
        Box::pin(StockExecutor::execute_progress(self, input, journal))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        StockExecutor::supports_native_execution(self, work)
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
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
            ).with_tool_authority(RuntimeScope::new(crate::Capabilities::new(grants), Limits::default())?, Arc::new(crate::test_support::FixedToolPolicy::new(crate::runtime::ToolPolicyDecision::Allow)?))?
                .without_tool_authority();
            prop_assert_eq!(executor.tool_scope.grants(), &expected);
            prop_assert_eq!(executor.tools.definitions(None)?, before);
            prop_assert!(stock_tool_definitions(&executor.tools, &executor.tool_scope, None)?.is_empty());
        }
    }
}
