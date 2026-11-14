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
    /// Saves the workspace before the delivery parks on `operation`.
    fn park<'a>(
        &'a self,
        _agent: &'a AgentState,
        _operation: OperationId,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    /// Publishes the workspace before this turn's terminal record.
    fn settle<'a>(
        &'a self,
        _agent: &'a AgentState,
        _turn: OperationId,
    ) -> BoxProviderFuture<'a, Result<()>> {
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

    /// Installs a per-step source after canonical turn composition and before
    /// the initial model's native presentation.
    /// The source captures canonical tools before native presentation, so a
    /// later family can build its declarations from the same admitted revisions.
    /// The running turn retains its original context and authority.
    fn configure_steps(
        &self,
        executor: StockExecutor,
        _agent: &AgentState,
        _authority: &TurnAuthority,
    ) -> Result<StockExecutor> {
        Ok(executor)
    }
    /// Workspace exposures and newly discovered instructions for this context.
    fn workspaces<'a>(
        &'a self,
        agent: &'a AgentState,
        seen: &'a std::collections::BTreeMap<crate::agent::EntryId, [u8; 32]>,
        content: &'a crate::runtime::ContentBindings,
        issuer: &'a crate::core::RuntimeAuthority,
        scope: &'a crate::core::Scope,
    ) -> BoxProviderFuture<'a, Result<Option<crate::agent::WorkspaceUpdate>>> {
        let _ = (agent, seen, content, issuer, scope);
        Box::pin(async { Ok(None) })
    }

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
            .with_execution_journal(Arc::clone(&authority.journal), turn)?
            .for_agent(agent.path.clone(), agent.volumes.clone())
            .with_agent_children(authority.children.clone());
        let executor = executor
            .with_tool_authority(context.scope().clone(), authority.approval.clone())?
            .with_task_context(&context, turn)?;
        Ok((executor, context))
    }
}

impl TurnContext for Tasks {
    fn park<'a>(
        &'a self,
        agent: &'a AgentState,
        operation: OperationId,
    ) -> BoxProviderFuture<'a, Result<()>> {
        self.settle(agent, operation)
    }
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
    binding: Arc<crate::model::ResolvedModel>,
    tools: ToolRegistry,
    models: Option<Arc<crate::model::ModelResolver>>,
    limits: Limits,
    compaction: Option<CompactionPolicy>,
    agent_tools: Vec<String>,
    templates: bool,
    question_plugin: bool,
    #[cfg(not(target_arch = "wasm32"))]
    plugins: Option<Arc<crate::plugins::Enabled>>,
    /// Settings sampling (`agent.ts`) for every model call.
    sampling: Option<crate::config::Sampling>,
    context: Arc<dyn TurnContext>,
}

impl StockExecutors {
    /// Use the session's admitted plugin set for every role and turn.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn with_plugins(mut self, plugins: Arc<crate::plugins::Enabled>) -> Self {
        self.plugins = Some(plugins);
        self
    }

    /// Binds child and resumed selections through the same protected catalog.
    #[must_use]
    pub fn with_models(mut self, models: Arc<crate::model::ModelResolver>) -> Self {
        self.models = Some(models);
        self
    }

    fn selected(&self, agent: &AgentState) -> Result<Arc<crate::model::ResolvedModel>> {
        match agent
            .profile
            .as_ref()
            .and_then(|profile| profile.model.as_deref())
        {
            None => Ok(Arc::clone(&self.binding)),
            Some(selection)
                if selection
                    .strip_prefix(self.binding.0.provider.as_str())
                    .and_then(|suffix| suffix.strip_prefix('/'))
                    == Some(self.binding.0.name.as_str()) =>
            {
                Ok(Arc::clone(&self.binding))
            }
            Some(selection) => self
                .models
                .as_ref()
                .ok_or_else(|| {
                    Error::Unsupported("model selection requires a host resolver".into())
                })?
                .resolve_shared(selection),
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
            binding: Arc::new(crate::model::ResolvedModel::new((model, provider))),
            tools,
            models: None,
            limits,
            compaction: compaction.into(),
            agent_tools: Vec::new(),
            templates: false,
            question_plugin: false,
            #[cfg(not(target_arch = "wasm32"))]
            plugins: None,
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
        let executor = match sampling.model_sampling() {
            Some(sampling) => executor.with_sampling(sampling)?,
            None => executor,
        };
        Ok(executor)
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

    /// Select the question plugin for new roots; each root pins this choice.
    #[must_use]
    pub fn with_question_plugin(mut self, enabled: bool) -> Self {
        self.question_plugin = enabled;
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
    fn park<'a>(
        &'a self,
        agent: &'a AgentState,
        operation: OperationId,
    ) -> BoxProviderFuture<'a, Result<()>> {
        self.context.park(agent, operation)
    }

    fn review<'a>(
        &'a self,
        agent: &'a AgentState,
        execution: (
            &'a dyn crate::executor::ExecutionJournal,
            Option<&'a crate::executor::PreparationIo>,
        ),
        request: (
            &'a crate::agent::AgentPath,
            &'a crate::approvals::ApprovalRequest<'a>,
        ),
        principal: &'a str,
        policy: &'a str,
    ) -> BoxProviderFuture<'a, Result<(crate::approvals::RuleVerdict, Option<String>)>> {
        Box::pin(async move {
            if !request.0.is_child_of(&agent.path) {
                return Err(Error::Unauthorized(
                    "review source is not a direct child".into(),
                ));
            }
            let binding = self.selected(agent)?;
            crate::approvals::review_model(
                execution,
                request.1,
                principal,
                (&binding.0, &binding.1),
                Some(policy),
                Some(request.0),
                self.limits,
            )
            .await
        })
    }

    fn question_plugin(&self) -> bool {
        self.question_plugin
    }
    fn model_selection(&self) -> Result<Option<String>> {
        crate::templates::root_model(
            &format!("{}/{}", self.binding.0.provider, self.binding.0.name),
            self.models.as_deref(),
        )
        .map(Some)
    }

    fn settle<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
    ) -> BoxProviderFuture<'a, Result<()>> {
        self.context.settle(agent, turn)
    }
    fn content_limits(&self) -> Limits {
        self.limits
    }

    fn templates(&self) -> bool {
        self.templates
    }

    fn workspaces<'a>(
        &'a self,
        agent: &'a AgentState,
        seen: &'a std::collections::BTreeMap<crate::agent::EntryId, [u8; 32]>,
        content: &'a crate::runtime::ContentBindings,
        issuer: &'a crate::core::RuntimeAuthority,
        scope: &'a crate::core::Scope,
    ) -> BoxProviderFuture<'a, Result<Option<crate::agent::WorkspaceUpdate>>> {
        self.context.workspaces(agent, seen, content, issuer, scope)
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
            let binding = self.selected(agent)?;
            let native = binding.native_tools()?;
            let directive = match (self.templates, &agent.profile) {
                (true, Some(creation)) => Some(
                    crate::templates::directive(creation, authority.content.reader.as_ref())
                        .await?,
                ),
                _ => None,
            };
            let mut tools = self.tools.clone();
            let question_plugin = agent
                .profile
                .as_ref()
                .is_some_and(|profile| profile.question_plugin);
            let question_allowed = question_plugin
                && directive
                    .as_ref()
                    .is_none_or(|directive| directive.question);
            if !question_allowed && tools.get("ask_question", None, None)?.is_some() {
                tools.remove_from_model("ask_question")?;
            }
            let agent_tools = self
                .agent_tools
                .iter()
                .filter(|name| {
                    agent
                        .profile
                        .as_ref()
                        .and_then(|profile| profile.topology.as_ref())
                        .is_none_or(|topology| topology.offers_tool(name, agent.parent.is_none()))
                })
                .cloned()
                .collect::<Vec<_>>();
            // The agent tools act for this turn: their effects commit with it.
            if let Some(effects) = &authority.effects
                && !self.agent_tools.is_empty()
            {
                let writer = authority
                    .journal
                    .output_content(turn)?
                    .and_then(|content| content.writer)
                    .ok_or_else(|| Error::Invalid("agent turn has no journal writer".into()))?;
                crate::tools::bind_agent_tools(
                    &mut tools,
                    agent,
                    (Arc::clone(effects), authority.children.clone()),
                    writer,
                    self.limits.file_bytes,
                    (&agent_tools, authority.templates.clone()),
                )?;
            }
            let executor = StockExecutor::from_binding(binding, ContextPipeline::default(), tools)
                .bind_content(authority.content.reader.clone(), self.limits.file_bytes)
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
            #[cfg(not(target_arch = "wasm32"))]
            let executor = {
                let plugins = match &self.plugins {
                    Some(plugins) => Arc::clone(plugins),
                    None => crate::plugins::seed().await?,
                };
                executor.with_plugins(plugins)?
            };
            // Context preparation may set the final output bound or an explicit
            // policy. Derive automatic budgets only after both are known.
            let executor = executor.with_automatic_compaction()?;

            let executor = if question_allowed {
                executor
            } else {
                executor.without_question_authority()?
            };
            let executor = match directive {
                Some(directive) => {
                    let executor = match directive.allowed_tools.as_deref() {
                        Some(allowed) => executor.with_tool_ceiling(allowed)?,
                        None => executor,
                    };
                    executor.with_context_stage(Arc::new(crate::context::SourceStage::new(
                        "template",
                        directive.digest,
                        directive.context,
                        crate::context::ContextPlacement::Prepend,
                    )))?
                }
                None => executor,
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
            let executor = self.context.configure_steps(executor, agent, authority)?;
            // Native declarations run the final canonical revisions and ceiling.
            let executor = match &native {
                Some(native) => executor.with_native_tools(native)?,
                None => executor,
            };
            Ok(Arc::new(executor) as Arc<dyn Executor>)
        })
    }
}
