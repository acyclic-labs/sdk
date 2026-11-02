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
                seed: None,
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
    native: std::sync::OnceLock<Option<Arc<crate::tools::native::NativeTools>>>,
    limits: Limits,
    compaction: CompactionPolicy,
    agent_tools: Vec<String>,
    templates: bool,
    max_children: Option<u32>,
    context: Arc<dyn TurnContext>,
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
            native: std::sync::OnceLock::new(),
            limits,
            compaction,
            agent_tools: Vec::new(),
            templates: false,
            max_children: None,
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

    /// Configures the host's explicit open-child capacity. `None` admits any
    /// number of children; it does not substitute a default limit.
    #[must_use]
    pub fn with_child_capacity(mut self, capacity: Option<u32>) -> Self {
        self.max_children = capacity;
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
    fn render_bytes(&self) -> Option<u64> {
        self.limits.render_bytes
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
                        self.max_children,
                        authority.templates.clone(),
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
            let executor = self
                .context
                .prepare((executor, self.limits), agent, turn, input, authority)
                .await?;
            // Native declarations run the revisions the prepared manifest pins.
            let executor = match &native {
                Some(native) => executor.with_native_tools(native)?,
                None => executor,
            };
            let executor = match (self.templates, &agent.profile) {
                (true, Some(creation)) => {
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
                        crate::templates::directive(creation, authority.content.reader.as_ref())
                            .await?,
                        crate::context::ContextPlacement::Prepend,
                    )))?
                }
                _ => executor,
            };
            Ok(Arc::new(executor) as Arc<dyn Executor>)
        })
    }
}
