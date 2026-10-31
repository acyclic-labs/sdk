//! The one stock composition of an agent turn's executor, shared by every
//! host: the local harness, the browser and a hosted server. A host supplies
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
    preset::Preset,
    runtime::{AgentHarness, RuntimeScope, TaskContext},
    tool::ToolRegistry,
};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

/// What one host adds to a stock turn once its model, tools, limits and
/// compaction are composed: at least the turn's tool authority.
pub trait TurnContext: acyclic_stream::ProviderPlatform + 'static {
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
            .for_agent(agent.path.clone(), agent.volumes.clone());
        let executor = executor
            .with_tool_authority(context.scope().clone(), authority.approval.clone())?
            .with_task_context(&context, turn)?;
        Ok((executor, context))
    }
}

impl TurnContext for Tasks {
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
}

impl TurnContext for Grants {
    fn prepare<'a>(
        &'a self,
        (executor, limits): (StockExecutor, Limits),
        _: &'a AgentState,
        _: OperationId,
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
            executor.with_tool_authority(
                RuntimeScope::new(grants, limits)?,
                authority.approval.clone(),
            )
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
    limits: Limits,
    compaction: CompactionPolicy,
    agent_tools: Vec<String>,
    profiles: Option<Arc<dyn crate::agent::SpawnProfiles>>,
    profile_models: Option<(
        Arc<crate::profile::Profiles>,
        Arc<dyn crate::profile::ProfileModels>,
    )>,
    context: Arc<dyn TurnContext>,
}

impl Preset {
    /// Stock turns over `model` served by `provider`, with
    /// the host's registry under `limits`, `compaction` and `context`.
    /// The shared agent tools apply; `limits`
    /// bounds execution capacity. The turn admission is the host's: pass the
    /// result to the turn host that carries it.
    #[must_use]
    pub fn executors(
        &self,
        model: (Model, Arc<dyn ModelProvider>),
        tools: ToolRegistry,
        (limits, compaction): (Limits, CompactionPolicy),
        context: Arc<dyn TurnContext>,
    ) -> StockExecutors {
        StockExecutors::new(model, tools, (limits, compaction), context)
            .with_agent_tools(&crate::tools::default_names())
    }

    /// The shared defaults, backed by the host (the volume tools
    /// always; each workspace tool only when `workspace` admits its name),
    /// then the host's own `tools`. Its agent tools are bound
    /// per turn by [`Self::executors`].
    ///
    /// # Errors
    /// When the preset names an unknown tool, or two tools share a name.
    pub fn tools(
        &self,
        workspace: impl Fn(&str) -> bool,
        tools: Vec<crate::tool::Tool>,
    ) -> Result<ToolRegistry> {
        let mut registry = ToolRegistry::default();
        crate::tools::bind_defaults(
            &mut registry,
            &mut Vec::new(),
            None,
            &crate::tools::default_names(),
            true,
            workspace,
        )?;
        for tool in tools {
            registry.register((tool, None), &mut None, None)?;
        }
        Ok(registry)
    }

    /// The call grant of every tool in `tools` and of this preset's agent
    /// tools, which bind per turn outside the registry.
    ///
    /// # Errors
    /// When a definition of `tools` cannot be produced.
    pub fn tool_grants(&self, tools: &ToolRegistry) -> Result<Vec<String>> {
        let names = crate::tools::default_names();
        let agent_tools = names
            .iter()
            .filter(|name| crate::tools::AGENT_TOOLS.contains(&name.as_str()))
            .cloned();
        Ok(tools
            .definitions(None)?
            .into_iter()
            .map(|definition| definition.name)
            .chain(agent_tools)
            .map(|name| crate::contract::capability::tool_call(&name))
            .collect())
    }
}

impl StockExecutors {
    /// Stock turns over `model` served by `provider`, with `tools` under
    /// `limits`, `compaction` and `context`, binding no agent tools.
    #[must_use]
    pub fn new(
        (model, provider): (Model, Arc<dyn ModelProvider>),
        tools: ToolRegistry,
        (limits, compaction): (Limits, CompactionPolicy),
        context: Arc<dyn TurnContext>,
    ) -> Self {
        Self {
            model,
            provider,
            tools,
            limits,
            compaction,
            agent_tools: Vec::new(),
            profiles: None,
            profile_models: None,
            context,
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

    /// Binds the original configured profile loader to child admission.
    #[must_use]
    pub fn with_profiles(mut self, profiles: Arc<dyn crate::agent::SpawnProfiles>) -> Self {
        self.profiles = Some(profiles);
        self
    }

    /// Selects each actor's admitted role model at the original durable turn
    /// identity. All execution still uses this stock composition.
    #[must_use]
    pub fn with_profile_models(
        mut self,
        profiles: Arc<crate::profile::Profiles>,
        models: Arc<dyn crate::profile::ProfileModels>,
    ) -> Self {
        self.profiles = Some(profiles.clone());
        self.profile_models = Some((profiles, models));
        self
    }
}

impl TurnExecutors for StockExecutors {
    fn executor<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            let (model, original_provider) = match &self.profile_models {
                Some((profiles, models)) => {
                    let pin = agent.profile.as_ref().ok_or_else(|| {
                        Error::Invalid("configured actor has no profile pin".into())
                    })?;
                    profiles.pinned(pin)?;
                    models.select(pin, turn, agent.conversation_tail)?
                }
                None => (self.model.clone(), self.provider.clone()),
            };
            let provider = original_provider
                .bind_content(authority.content.reader.clone(), self.limits.file_bytes)
                .unwrap_or(original_provider);
            // The agent tools act for this turn: their effects commit with it.
            let mut tools = self.tools.clone();
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
                    (&self.agent_tools, self.profiles.clone()),
                )?;
            }
            let executor = StockExecutor::new(model, provider, ContextPipeline::default(), tools)
                .with_limits(self.limits)
                .with_compaction_policy(self.compaction.clone());
            let executor = self
                .context
                .prepare((executor, self.limits), agent, turn, input, authority)
                .await?;
            let executor = if let Some((profiles, _)) = &self.profile_models {
                executor.with_profile(
                    profiles.clone(),
                    agent.profile.clone().ok_or_else(|| {
                        Error::Invalid("configured actor has no profile pin".into())
                    })?,
                )?
            } else {
                executor
            };
            Ok(Arc::new(executor) as Arc<dyn Executor>)
        })
    }
}
