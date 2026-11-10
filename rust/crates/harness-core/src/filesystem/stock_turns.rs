//! The one stock composition of an agent turn's executor, shared by every
//! host: the local harness, the browser and an agent Actor. A host supplies
//! its model, tools and limits, and a [`TurnContext`] for what only it adds;
//! the turn's admission is the host's own, carried by its
//! [`super::FilesystemTurnHost`] or [`super::hosting::Bound`], never chosen
//! here.
use super::{TurnAuthority, TurnExecutors};
use crate::{
    Capabilities, Error, OperationId, Result,
    agent::AgentState,
    context::{CompactionPolicy, ContextPipeline},
    conversation::Limits,
    executor::{Executor, StockExecutor},
    model::{Model, ModelContent, ModelProvider},
    runtime::{AgentHarness, RuntimeScope, TaskContext},
    tool::ToolRegistry,
};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

/// What one host adds to a stock turn once its model, tools, limits and
/// compaction are composed: at least the turn's tool authority.
pub trait TurnContext: acyclic_stream::ProviderPlatform + 'static {
    /// Publishes the workspace before this turn's terminal record.
    fn settle(&self, _agent: &AgentState, _turn: OperationId) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    /// Completes `composed`, the executor and the limits it was composed
    /// under, for `agent`'s turn `turn` on `input`.
    fn prepare<'a>(
        &'a self,
        composed: (StockExecutor, Limits),
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, Result<StockExecutor>>;

    /// The execution targets `agent`'s turns run on
    /// ([`TurnExecutors::roster`]); `None` binds no targets.
    fn roster(&self, agent: &AgentState) -> Option<Arc<crate::agent::Roster>> {
        let _ = agent;
        None
    }
}

/// The context of a host with no context of its own: each turn's tools act
/// under exactly the grants its admission gives it, plus `grants`, and the
/// model may generate.
pub struct Grants(pub Vec<String>);

/// Stock turns backed by the opener's immutable runtime bindings. Workspace
/// tools receive the turn's original content, journal and task identity.
pub struct Tasks(pub Arc<AgentHarness>);

impl Tasks {
    /// Binds one executor and returns the same task context for host hooks.
    pub fn bind(
        harness: &Arc<AgentHarness>,
        executor: StockExecutor,
        agent: &AgentState,
        turn: OperationId,
        authority: &TurnAuthority,
    ) -> Result<(StockExecutor, TaskContext)> {
        let context = harness
            .agent_context(&authority.grants, authority.content.clone())?
            .with_execution_journal(Arc::clone(&authority.journal))
            .for_agent(agent.path.clone(), agent.volumes.clone())
            .with_agent_children(authority.children.clone());
        let executor = executor
            .with_tool_authority(context.scope().clone(), authority.approval.clone())?
            .with_task_context(&context, turn)?;
        Ok((executor, context))
    }
}

impl TurnContext for Tasks {
    fn settle(&self, _: &AgentState, turn: OperationId) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            if let Some(targets) = self.0.targets() {
                for target in targets.roster().targets() {
                    targets
                        .recover(&target.name)?
                        .executor
                        .settle_workspace(turn)
                        .await?;
                }
            }
            Ok(())
        })
    }
    fn prepare<'a>(
        &'a self,
        (executor, _): (StockExecutor, Limits),
        agent: &'a AgentState,
        turn: OperationId,
        _: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, Result<StockExecutor>> {
        Box::pin(async move {
            Self::bind(&self.0, executor, agent, turn, authority).map(|(executor, _)| executor)
        })
    }

    fn roster(&self, _: &AgentState) -> Option<Arc<crate::agent::Roster>> {
        self.0.targets().map(|targets| targets.roster())
    }
}

impl TurnContext for Grants {
    fn prepare<'a>(
        &'a self,
        (executor, limits): (StockExecutor, Limits),
        agent: &'a AgentState,
        turn: OperationId,
        _: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, Result<StockExecutor>> {
        Box::pin(async move {
            let grants = Capabilities::new(
                authority
                    .grants
                    .iter()
                    .map(str::to_owned)
                    .chain(self.0.iter().cloned())
                    .chain([crate::contract::capability::MODEL_GENERATE.to_owned()]),
            );
            let scope = RuntimeScope::new(grants, limits)?;
            if authority.workspace_tools.is_none() {
                return executor.with_tool_authority(scope, authority.approval.clone());
            }
            let harness = crate::runtime::Bindings {
                filesystem: authority.workspace_tools.clone(),
                scope,
                content: Some(authority.content.clone()),
                ..crate::runtime::Bindings::local()
            }
            .build()?;
            Tasks::bind(&harness, executor, agent, turn, authority).map(|(executor, _)| executor)
        })
    }
}

/// Every agent turn's stock executor: the model reading the turn's content,
/// the tools with the turn's agent tools bound to its effects, the limits and
/// compaction, then the host's [`TurnContext`].
pub struct StockExecutors {
    model: Model,
    provider: Arc<dyn ModelProvider>,
    tools: ToolRegistry,
    /// The model family's native declarations, decoded on the first turn.
    native: crate::tools::native::Cache,
    models: Option<Arc<crate::model::ModelResolver>>,
    limits: Limits,
    compaction: Option<CompactionPolicy>,
    agent_tools: Vec<String>,
    templates: bool,
    /// Settings sampling (`agent.ts`) for every model call.
    sampling: Option<crate::config::Sampling>,
    context: Arc<dyn TurnContext>,
}

impl StockExecutors {
    /// Binds child and resumed selections through the same protected catalog.
    #[must_use]
    pub fn with_models(mut self, models: Arc<crate::model::ModelResolver>) -> Self {
        self.models = Some(models);
        self
    }

    fn selected(&self, agent: &AgentState) -> Result<crate::model::ModelBinding> {
        match agent
            .profile
            .as_ref()
            .and_then(|profile| profile.model.as_deref())
        {
            None => Ok((self.model.clone(), self.provider.clone())),
            Some(selection)
                if selection
                    .strip_prefix(self.model.provider.as_str())
                    .and_then(|suffix| suffix.strip_prefix('/'))
                    == Some(self.model.name.as_str()) =>
            {
                Ok((self.model.clone(), self.provider.clone()))
            }
            Some(selection) => self
                .models
                .as_ref()
                .ok_or_else(|| {
                    Error::Unsupported("model selection requires a host resolver".into())
                })?
                .resolve(selection),
        }
    }

    /// Stock turns over `model` served by `provider`, with `tools` under
    /// `limits`, `compaction` and `context`, binding no agent tools. `None`
    /// selects automatic compaction budgets separately for each selected model.
    #[must_use]
    pub fn new(
        (model, provider): (Model, Arc<dyn ModelProvider>),
        tools: ToolRegistry,
        (limits, compaction): (Limits, impl Into<Option<CompactionPolicy>>),
        context: Arc<dyn TurnContext>,
    ) -> Self {
        Self {
            model,
            provider,
            tools,
            native: Default::default(),
            models: None,
            limits,
            compaction: compaction.into(),
            agent_tools: Vec::new(),
            templates: false,
            sampling: None,
            context,
        }
    }

    /// Calls the model with `sampling` on every turn. Admission
    /// ([`crate::config::AgentConfig`]) has already checked its numbers; a
    /// turn re-checks them.
    #[must_use]
    pub fn with_sampling(mut self, sampling: crate::config::Sampling) -> Self {
        self.sampling = Some(sampling);
        self
    }

    fn sample(executor: StockExecutor, sampling: crate::config::Sampling) -> Result<StockExecutor> {
        let executor = executor.with_sampling(crate::model::ModelSampling {
            seed: None,
            temperature: sampling.temperature,
            top_p: sampling.top_p,
        })?;
        match sampling.max_output_tokens {
            Some(maximum) => executor.with_max_output_tokens(maximum),
            None => Ok(executor),
        }
    }

    /// Binds the agent tools among `names` to each turn's effects.
    #[must_use]
    pub fn with_agent_tools(mut self, names: &[String]) -> Self {
        self.agent_tools = names
            .iter()
            .filter(|name| crate::tools::AGENT_TOOLS.contains(&name.as_str()))
            .cloned()
            .collect();
        self
    }

    /// Reads creation templates through the original filesystem admission.
    #[must_use]
    pub fn with_templates(mut self) -> Self {
        self.templates = true;
        self
    }
}

impl TurnExecutors for StockExecutors {
    fn model_selection(&self) -> Option<String> {
        Some(format!("{}/{}", self.model.provider, self.model.name))
    }

    fn settle<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
    ) -> BoxProviderFuture<'a, Result<()>> {
        self.context.settle(agent, turn)
    }
    fn render_bytes(&self) -> Option<u64> {
        Some(self.limits.render_bytes)
    }

    fn templates(&self) -> bool {
        self.templates
    }

    fn roster(&self, agent: &AgentState) -> Option<Arc<crate::agent::Roster>> {
        self.context.roster(agent)
    }

    fn executor<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            let (model, provider) = self.selected(agent)?;
            let native = self.native.get(&model, || provider.native_tools(&model))?;
            let provider = provider
                .bind_content(authority.content.reader.clone(), self.limits.file_bytes)
                .unwrap_or(provider);
            let mut tools = self.tools.clone();
            // The agent tools act for this turn: their effects commit with it.
            if let Some(effects) = &authority.effects
                && !self.agent_tools.is_empty()
            {
                let writer = authority
                    .content
                    .writer
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("agent turn has no private writer".into()))?;
                crate::tools::bind_agent_tools(
                    &mut tools,
                    agent,
                    (Arc::clone(effects), authority.children.clone()),
                    Arc::clone(writer),
                    self.limits.file_bytes,
                    (&self.agent_tools, authority.templates.clone()),
                )?;
            }
            let executor = StockExecutor::new(model, provider, ContextPipeline::default(), tools)
                .with_limits(self.limits);
            let executor = match &self.compaction {
                Some(policy) => executor.with_compaction_policy(policy.clone()),
                None => executor,
            };
            let executor = match self.sampling {
                Some(sampling) => Self::sample(executor, sampling)?,
                None => executor,
            };
            let executor = self
                .context
                .prepare((executor, self.limits), agent, turn, input, authority)
                .await?;
            // Context preparation may set the final output bound or an explicit
            // policy. Derive automatic budgets only after both are known.
            let executor = executor.with_automatic_compaction()?;
            // Native declarations run the revisions the prepared manifest pins.
            let executor = match &native {
                Some(native) => executor.with_native_tools(native)?,
                None => executor,
            };
            let executor = match (self.templates, &agent.profile) {
                (true, Some(creation)) => {
                    let directive =
                        crate::templates::directive(creation, authority.content.reader.as_ref())
                            .await?;
                    let executor = match directive.allowed_tools.as_deref() {
                        Some(allowed) => executor.with_tool_ceiling(allowed)?,
                        None => executor,
                    };
                    let digest = creation.template.as_ref().map_or_else(
                        || {
                            blake3::hash(include_bytes!("../seed/templates/orchestrator/AGENT.md"))
                                .to_hex()
                                .to_string()
                        },
                        |file| hex::encode(file.descriptor().sha256()),
                    );
                    executor.with_context_stage(Arc::new(crate::context::SourceStage::new(
                        "template",
                        digest,
                        directive.context,
                        crate::context::ContextPlacement::Prepend,
                    )))?
                }
                _ => executor,
            };
            let intervention = agent
                .profile
                .as_ref()
                .and_then(|profile| profile.continuation.as_ref())
                .filter(|_| agent.conversation_tail == agent.conversation_base)
                .and_then(|continuation| continuation.override_item.as_ref());
            let executor = match intervention {
                Some(file) => {
                    let bytes = authority
                        .content
                        .reader
                        .read(file, file.descriptor().byte_length(), None, None)?
                        .await?;
                    let request: crate::agent::ContinuationRequest = serde_json::from_slice(&bytes)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    if request.version != 1 {
                        return Err(Error::Invalid("unsupported continuation version".into()));
                    }
                    match request.override_item {
                        Some(item) => executor.with_intervention(item),
                        None => executor,
                    }
                }
                None => executor,
            };
            Ok(Arc::new(executor) as Arc<dyn Executor>)
        })
    }
}
