//! The one stock composition of an agent turn's executor, shared by every
//! host: the local harness, the browser and an agent Actor. A host supplies
//! its model, tools and limits, and a [`TurnContext`] for what only it adds;
//! the turn's admission is the host's own, carried by its
//! [`super::FilesystemTurnHost`] or [`super::hosting::Bound`], never chosen
//! here.
use super::{TurnAuthority, TurnExecutors};
use crate::{
    Capabilities, Error, OperationId, Result,
    agent::{AgentState, SpawnProfiles},
    context::{CompactionPolicy, ContextPipeline},
    conversation::Limits,
    executor::{Executor, StockExecutor},
    model::{Model, ModelContent, ModelProvider},
    preset::Preset,
    profile::Profiles,
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
                targets: None,
                filesystem: authority.workspace_tools.clone(),
                scope,
                content: Some(authority.content.clone()),
                artifacts: None,
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
    #[cfg(not(target_arch = "wasm32"))]
    session: super::session::SessionBinding,
    model: Model,
    provider: Arc<dyn ModelProvider>,
    tools: ToolRegistry,
    /// The model family's native declarations, decoded on the first turn.
    native: std::sync::OnceLock<Option<Arc<crate::tools::native::NativeTools>>>,
    limits: Limits,
    compaction: CompactionPolicy,
    agent_tools: Vec<String>,
    profiles: Option<Arc<Profiles>>,
    context: Arc<dyn TurnContext>,
}

impl Preset {
    /// This preset's stock turns over `model` served by `provider`, with
    /// `tools` (the host's registry of this preset's tools) under `limits`,
    /// `compaction` and `context`. The preset's agent tools apply; a host
    /// that runs children adds role profiles with
    /// [`StockExecutors::with_profiles`]. `limits` bounds execution capacity. The turn admission is the host's: pass the
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
            .with_agent_tools(&self.tools)
    }

    /// This preset's tools, the one construction every host takes them from:
    /// the catalog tools it selects that the host can back (the volume tools
    /// and filesystem commands always; each workspace tool only when
    /// `workspace` admits its name),
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
            &self.tools,
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
        let agent_tools = self
            .tools
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
            #[cfg(not(target_arch = "wasm32"))]
            session: super::session::SessionBinding::default(),
            model,
            provider,
            tools,
            native: std::sync::OnceLock::new(),
            limits,
            compaction,
            agent_tools: Vec::new(),
            profiles: None,
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

    /// Runs each agent under its pinned role in `profiles`: roots are created
    /// with the top-level role, spawns admit children by role, and each turn
    /// carries its role's directive.
    #[must_use]
    pub fn with_profiles(mut self, profiles: Arc<Profiles>) -> Self {
        self.profiles = Some(profiles);
        self
    }
}

impl TurnExecutors for StockExecutors {
    #[cfg(not(target_arch = "wasm32"))]
    fn admit_session(&self, session: &Arc<super::SessionWork>) -> Result<()> {
        self.session.admit(session)
    }

    fn render_bytes(&self) -> Option<u64> {
        Some(self.limits.render_bytes)
    }

    fn profiles(&self) -> Option<Arc<dyn SpawnProfiles>> {
        self.profiles
            .clone()
            .map(|profiles| profiles as Arc<dyn SpawnProfiles>)
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
            let provider = self
                .provider
                .bind_content(authority.content.reader.clone(), self.limits.file_bytes)
                .unwrap_or_else(|| Arc::clone(&self.provider));
            let native = match self.native.get() {
                Some(native) => native.clone(),
                None => {
                    let native = self
                        .provider
                        .native_tool_profile(&self.model)?
                        .map(crate::tools::native::NativeTools::new)
                        .transpose()?
                        .map(Arc::new);
                    self.native.get_or_init(|| native).clone()
                }
            };
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
                    (
                        &self.agent_tools,
                        self.profiles
                            .clone()
                            .map(|profiles| profiles as Arc<dyn SpawnProfiles>),
                    ),
                )?;
            }
            let executor = StockExecutor::new(
                self.model.clone(),
                provider,
                ContextPipeline::default(),
                tools,
            )
            .with_limits(self.limits)
            .with_compaction_policy(self.compaction.clone());
            #[cfg(not(target_arch = "wasm32"))]
            let executor = executor.with_session(
                self.session
                    .get()?
                    .map(|session| session.agent(&agent.path.id()))
                    .transpose()?,
            );
            let executor = self
                .context
                .prepare((executor, self.limits), agent, turn, input, authority)
                .await?;
            // Native declarations run the revisions the prepared manifest pins.
            let executor = match &native {
                Some(native) => executor.with_native_tools(native)?,
                None => executor,
            };
            let executor = match (&self.profiles, &agent.profile) {
                (Some(profiles), Some(pin)) => {
                    executor.with_context_stage(Arc::new(crate::context::SourceStage::new(
                        "role",
                        blake3::Hash::from(pin.digest).to_hex().to_string(),
                        profiles.directive(pin)?,
                        crate::context::ContextPlacement::Prepend,
                    )))?
                }
                _ => executor,
            };
            Ok(Arc::new(executor) as Arc<dyn Executor>)
        })
    }
}
