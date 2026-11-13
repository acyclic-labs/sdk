use super::*;

impl StockExecutor {
    /// Retains a selected executable revision from this executor's admitted
    /// catalog. This observation grants no execution authority; derived file
    /// programs still use ordinary canonical admission and the original scope.
    pub fn tool_binding(&self, name: &str) -> Result<Option<Arc<crate::tool::Tool>>> {
        Ok(self.tools.get_shared(name, None, None)?.map(Arc::clone))
    }
    /// Retains only the complete model/tool selection for a later step. This
    /// cannot replace the running turn's context, authority or journal.
    /// Shared storage uses the supplied original preparation owner.
    pub fn into_step_binding(self, io: Option<&PreparationIo>) -> Result<Arc<StepBinding>> {
        let attempt = io.map(PreparationIo::scoped).transpose()?;
        self.into_step_binding_scoped(attempt.as_ref(), None)
    }

    fn into_step_binding_scoped(
        self,
        storage: Option<&PreparationIo>,
        effort: Option<crate::config::Effort>,
    ) -> Result<Arc<StepBinding>> {
        let bytes = acyclic_fs::shared_allocation_bytes::<StepBinding>().map_err(|_| {
            stock_boundary(
                storage,
                crate::wire::ErrorCode::Invalid,
                "step binding storage overflows",
            )
        })?;
        crate::executor::with_history_io(storage, |work| -> Result<()> {
            if let Some(work) = work.as_deref_mut() {
                work.retain_owned(bytes)?;
                work.allocation_attempt(bytes)?;
            }
            Ok(())
        })?;
        let custody = storage
            .map(PreparationIo::take_all_owned_custody)
            .transpose()?;
        Ok(Arc::new(StepBinding {
            model: self.model,
            sampling: self.sampling,
            effort,
            resolved_sampling: self.resolved_sampling,
            provider: self.provider,
            tools: self.tools,
            #[cfg(not(target_arch = "wasm32"))]
            hooks: self.hooks,
            #[cfg(not(target_arch = "wasm32"))]
            host_hooks: self.host_hooks,
            #[cfg(not(target_arch = "wasm32"))]
            plugin_set: self.plugin_set,
            #[cfg(not(target_arch = "wasm32"))]
            prompt_block: self.prompt_block,
            _custody: custody,
        }))
    }

    /// Captures the canonical configuration once before native presentation.
    /// Later selections borrow the same revisions and hooks; the running turn
    /// keeps its original context, authority and journal.
    pub fn step_template(&self) -> StepBinding {
        StepBinding {
            model: self.model.clone(),
            sampling: self.sampling,
            effort: None,
            resolved_sampling: once_cell::sync::OnceCell::new(),
            provider: Arc::clone(&self.provider),
            tools: self.tools.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            hooks: self.hooks.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            host_hooks: self.host_hooks.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            plugin_set: self.plugin_set.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            prompt_block: self.prompt_block.clone(),
            _custody: None,
        }
    }
    /// Applies a host-admitted complete configuration before each model step.
    /// The source pins its original file selection in this execution's journal;
    /// model and tool dispatch continue through the same stock engine.
    #[must_use]
    pub fn with_step_configuration(mut self, source: Arc<dyn StepConfiguration>) -> Self {
        self.step_configuration = Some(source);
        self
    }

    /// Install admitted plugin descriptions and pure after-hooks. Descriptions
    /// are a stable prefix; settlement notices append to the journaled turn.
    /// Turn and step snapshots share the original admitted owner; executable
    /// hooks and their allocation custody are never copied into a snapshot.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_plugins(mut self, plugins: Arc<crate::plugins::Enabled>) -> Result<Self> {
        self.plugin_set = Some(Arc::clone(&plugins));
        if plugins.descriptions.is_empty() {
            return Ok(self);
        }
        let text = plugins.descriptions.join("\n");
        let digest = blake3::hash(text.as_bytes()).to_hex().to_string();
        self.with_context_stage(Arc::new(crate::context::SourceStage::new(
            "plugins",
            digest,
            Arc::new(crate::context::Context {
                messages: vec![ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(text),
                }],
                ..Default::default()
            }),
            crate::context::ContextPlacement::Prepend,
        )))
    }

    /// Adds an original host-owned tool source. Context declarations remain
    /// pinned; lazy file programs load only when code selects their names.
    #[must_use]
    pub fn with_tool_source(mut self, source: Arc<dyn crate::tool::ToolSource>) -> Self {
        self.tools = self.tools.with_source(source);
        self
    }
    #[cfg(not(target_arch = "wasm32"))]
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
        Self::from_binding(
            Arc::new(crate::model::ResolvedModel::new((model, provider))),
            context,
            tools,
        )
    }

    /// Retains a protected catalog's immutable model/provider pair. Model
    /// metadata is borrowed until request preparation copies it on the
    /// execution's original allowance.
    #[must_use]
    pub fn from_binding(
        binding: Arc<crate::model::ResolvedModel>,
        context: ContextPipeline,
        tools: ToolRegistry,
    ) -> Self {
        let provider = Arc::clone(&binding.1);
        Self {
            step_configuration: None,
            intervention: None,
            model: ExecutorModel(binding),
            sampling: Default::default(),
            resolved_sampling: once_cell::sync::OnceCell::new(),
            provider,
            context,
            tools,
            epoch_tools: None,
            #[cfg(not(target_arch = "wasm32"))]
            hooks: None,
            #[cfg(not(target_arch = "wasm32"))]
            host_hooks: None,
            #[cfg(not(target_arch = "wasm32"))]
            plugin_set: None,
            #[cfg(not(target_arch = "wasm32"))]
            prompt_block: None,
            limits: Limits::default(),
            tool_scope: RuntimeScope::default(),
            policy: None,
            policy_identity: None,
            inherited_prefix: None,
            task_context: None,
            compaction: None,
            context_management: false,
            reconciliation_only: false,
        }
    }

    pub(crate) fn bind_content(
        mut self,
        reader: Arc<dyn crate::conversation::ContentResidencyVerifier>,
        maximum: Option<u64>,
    ) -> Self {
        if let Some(provider) = self.provider.bind_content(reader, maximum) {
            self.provider = provider;
        }
        self
    }

    /// Injects one instruction or replaces the first response step. The ordinary
    /// journal retains the delta; later steps use the original provider.
    #[must_use]
    pub fn with_intervention(mut self, item: crate::agent::ContinuationOverride) -> Self {
        self.intervention = Some(Arc::new(item));
        self
    }

    /// Pins explicit sampling for an executor without an admitted model profile.
    pub fn with_sampling(mut self, sampling: crate::model::ModelSampling) -> Result<Self> {
        sampling.validate()?;
        self.sampling = Some(sampling);
        self.resolved_sampling = once_cell::sync::OnceCell::new();
        Ok(self)
    }

    /// Replaces or disables automatic compaction. Disabled compaction sends
    /// ordinary requests unchecked; single inference keeps capacity admission.
    #[must_use]
    pub fn with_compaction_policy(mut self, policy: crate::context::CompactionPolicy) -> Self {
        self.compaction = Some(policy);
        self
    }

    pub(crate) fn with_automatic_compaction(mut self) -> Result<Self> {
        if self.compaction.is_none() {
            self.compaction = Some(crate::context::CompactionPolicy::for_model(
                None,
                &self.model,
                self.provider.as_ref(),
            )?);
        }
        self.context_management = !matches!(
            self.compaction,
            Some(crate::context::CompactionPolicy::Disabled)
        );
        Ok(self)
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

    /// Binds request assembly and fixed tool selection to one frozen context
    /// epoch. Its manifest remains the prefix; live tool sources retain their
    /// own generations and announce changes through the ordinary tool notices.
    /// The host manifest reader never reads source bodies; their original
    /// reader retains the task's source authority.
    pub async fn with_context_epoch(
        mut self,
        epoch: &crate::context::ContextEpoch,
        manifest_reader: Arc<dyn crate::conversation::ContentResidencyVerifier>,
        reader: Arc<dyn crate::conversation::ContentResidencyVerifier>,
    ) -> Result<Self> {
        let reference = epoch.manifest().clone();
        if self.context.contains_stage("context-manifest") {
            return Err(Error::Conflict(
                "executor already has a context manifest".into(),
            ));
        }
        let (tools, stage, baseline) = crate::context::ContextManifest::bind_observed(
            reference,
            manifest_reader.as_ref(),
            reader.as_ref(),
            self.tools,
            self.limits,
            None,
            false,
        )
        .await?;
        self.epoch_tools = Some(Arc::new(baseline));
        self.tools = tools;
        self.with_context_stage(stage)
    }

    /// Offers the selected capabilities as `native`'s declarations. Called
    /// after any context manifest pins the selection, which a bound registry
    /// refuses, so each declaration runs the revision the manifest pins.
    pub(crate) fn with_native_tools(
        mut self,
        native: &crate::tools::native::NativeTools,
    ) -> Result<Self> {
        native.bind(&mut self.tools)?;
        Ok(self)
    }

    /// Removes fresh tool-call grants while preserving the pinned catalog and admitted versions.
    pub(crate) fn without_tool_authority(mut self) -> Self {
        self.tools.restrict_sources(&[]);
        self.tool_scope
            .grants
            .0
            .retain(|grant| !grant.starts_with("tool:call:"));
        self
    }

    /// Optional plugin authority is granted only by the pinned role file.
    pub(crate) fn without_question_authority(mut self) -> Result<Self> {
        self.tool_scope
            .grants
            .0
            .retain(|grant| grant != "tool:call:ask_question");
        if let Some((context, operation)) = &self.task_context {
            self.task_context = Some((
                Arc::new(context.scoped(self.tool_scope.grants().clone(), self.limits)?),
                *operation,
            ));
        }
        Ok(self)
    }

    /// Intersect an immutable role ceiling with both model and executable
    /// authority. A missing grant is never supplied by a template's prose.
    pub(crate) fn with_tool_ceiling(mut self, allowed: &[String]) -> Result<Self> {
        self.tools.restrict_sources(allowed);
        self.tool_scope.grants.0.retain(|grant| {
            grant
                .strip_prefix("tool:call:")
                .is_none_or(|name| allowed.iter().any(|allowed| allowed == name))
        });
        if let Some((context, operation)) = &self.task_context {
            self.task_context = Some((
                Arc::new(context.scoped(self.tool_scope.grants().clone(), self.limits)?),
                *operation,
            ));
        }
        Ok(self)
    }
}

impl StockExecutor {
    /// Retains this exact composition while refusing every fresh effect intent.
    #[cfg(test)]
    pub(crate) fn into_reconciliation(mut self) -> Self {
        self.reconciliation_only = true;
        self
    }
}

impl StockExecutor {
    /// Returns the selected model's published capacity without imposing a budget.
    pub fn context_capacity(&self) -> Result<crate::context::ModelContextCapacity> {
        self.provider.context_capacity(&self.model)
    }

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
    /// Checks task authority here; model-summary resources are admitted at
    /// model entry, independently of host-selected tool execution.
    pub fn with_task_context(
        mut self,
        context: &crate::runtime::TaskContext,
        execution_operation: OperationId,
    ) -> Result<Self> {
        let context = context.scoped(self.tool_scope.grants().clone(), self.limits)?;
        // Children render their parent's prefix, so the whole agent tree
        // shares one placement: a child's first request reads what its
        // ancestors cached.
        let conversation = context.agent().map_or(execution_operation, |(agent, _)| {
            OperationId::from_bytes(agent.root_id_bytes())
        });
        if let Some(provider) = self.provider.bind_conversation(conversation) {
            self.provider = provider;
        }
        self.task_context = Some((Arc::new(context), execution_operation));
        StockExecution::ordinary(&self).verify_task_authority_binding(execution_operation)?;
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

    #[cfg(not(target_arch = "wasm32"))]
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
                Model::new("test".into(), "sampling".into(), "1".into())?,
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

impl StepBinding {
    /// Original protected selection retained by this turn's canonical template.
    pub fn model_binding(&self) -> &Arc<crate::model::ResolvedModel> {
        &self.model.0
    }

    /// Binds a complete admitted step through the same canonical composition.
    /// Imported host rules retain their original order and authority ahead of
    /// selected plugin rules. Prompt events and prefix remain with the turn.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn bind_configuration(
        &self,
        (model, sampling, effort): (
            Arc<crate::model::ResolvedModel>,
            Option<crate::model::ModelSampling>,
            Option<crate::config::Effort>,
        ),
        plugins: Arc<crate::plugins::Enabled>,
        reader: Arc<dyn crate::conversation::ContentResidencyVerifier>,
        maximum_bytes: Option<u64>,
        io: Option<&PreparationIo>,
    ) -> Result<Arc<StepBinding>> {
        let attempt = io.map(PreparationIo::scoped).transpose()?;
        let temporary = attempt.as_ref();
        let native = model.native_tools()?;
        let mut executor =
            StockExecutor::from_binding(model, ContextPipeline::default(), self.tools.clone())
                .bind_content(reader, maximum_bytes);
        if let Some(sampling) = sampling {
            executor = executor.with_sampling(sampling)?;
        }
        {
            let empty = crate::hooks::Hooks::default();
            let host = self.host_hooks.as_deref().unwrap_or(&empty);
            if host.has_execution_hooks() || plugins.rules.has_execution_hooks() {
                executor = executor.with_hooks(Some(crate::hooks::bind_rules(
                    host,
                    &plugins.rules,
                    temporary,
                )?));
            }
            executor.host_hooks = self.host_hooks.clone();
            executor.plugin_set = Some(plugins);
            executor.prompt_block = self
                .prompt_block
                .as_ref()
                .map(|reason| stock_string_copy(reason, temporary))
                .transpose()?;
        }
        if let Some(native) = native {
            executor = executor.with_native_tools(&native)?;
        }
        executor.into_step_binding_scoped(temporary, effort)
    }
}
