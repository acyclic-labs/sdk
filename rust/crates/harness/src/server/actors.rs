//! Stock agent turns over the public Actors runtime and an injected placement.
use super::{Composition, Hosted, Verifier};
pub use crate::hosting::Runtime;
use crate::{
    Error,
    agent::IntentId,
    context::CompactionPolicy,
    conversation::Limits,
    core::AuthorityRoot,
    filesystem::{FilesystemHost, Grants, TurnContext, TurnLimits},
    hosting::{self, Bound, Placement, RootRequest, Serving, Turns, VerifiedCaller},
    model::{Model, ModelProvider},
    preset::Preset,
    tool::Tool,
};
use acyclic_actors::{domain::ActorLimits, local::SystemClock};
use acyclic_fs::{AsyncObjectStore, distributed::AuthorityProvider};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

/// Execution and explicit policies supplied by the host.
pub struct Execution {
    /// The selected model and its production provider; credentials stay there.
    pub model: (Model, Arc<dyn ModelProvider>),
    /// Model, content and tool policy.
    pub limits: Limits,
    /// The turn's filesystem and history policy.
    pub turns: TurnLimits,
    /// Explicit Actors execution policy.
    pub actors: ActorLimits,
    /// Compaction selected for the model's context window.
    pub compaction: CompactionPolicy,
    /// Host tools in addition to the preset's stock tools.
    pub tools: Vec<Tool>,
    /// Whether this host supplies native workspace tools.
    pub workspace: bool,
    /// Additional task context, when the host supplies one. Otherwise tools
    /// receive exactly this turn's admitted grants and the selected tool grants.
    pub context: Option<Arc<dyn TurnContext>>,
    /// Independent models selected by the approval policy's named LLM stages.
    pub approvers: crate::approvals::ApproverModels,
    /// The host's goal continuation policy; `None` runs no automatic goals.
    pub goals: Option<crate::goal::GoalContinuationPolicy>,
}

/// A managed or self-deployed Actors composition for [`super::serve`].
/// Storage, caller verification and model I/O are injected; turn composition,
/// admission, root publication, spawn and serving have one implementation.
pub struct Actors<P: AuthorityProvider + 'static, O: 'static> {
    /// Host-held signing custody.
    pub issuer: AuthorityRoot,
    /// The platform's verified caller, never a request-supplied identity.
    pub caller: VerifiedCaller,
    /// One provider for agent state and volume authority.
    pub placement: Placement<P, O>,
    /// Stable root identity, volumes and creation intent.
    pub root: (IntentId, RootRequest),
    /// GraphCoder defaults, or the host's explicit overrides.
    pub preset: Preset,
    /// The model and tools running each admitted turn.
    pub execution: Execution,
    /// Current command authorization and durable wake admission.
    pub platform: Arc<dyn hosting::Platform>,
}

impl<P: AuthorityProvider + 'static, O: 'static> Actors<P, O> {
    /// GraphCoder on Actors, without a native workspace or a second agent loop.
    #[must_use]
    pub fn graphcoder(
        issuer: AuthorityRoot,
        caller: VerifiedCaller,
        placement: Placement<P, O>,
        root: (IntentId, RootRequest),
        execution: Execution,
        platform: Arc<dyn hosting::Platform>,
    ) -> Self {
        Self {
            issuer,
            caller,
            placement,
            root,
            preset: Preset::graphcoder(),
            execution,
            platform,
        }
    }
}

/// Startup failed, or retained the exact uncertain root for caller-owned recovery.
#[derive(thiserror::Error)]
pub enum ActorsError<P: AuthorityProvider + 'static, O: AsyncObjectStore + 'static> {
    /// A definitive opening or serving failure.
    #[error(transparent)]
    Failed(#[from] Error),
    /// No second request is prepared. Retain this composition and open it only
    /// when the provider can make progress, or drop it on cancellation.
    #[error("root publication unknown; retain and reopen the pending Actors composition")]
    Unknown(Box<PendingActors<P, O>>),
}

impl<P: AuthorityProvider + 'static, O: AsyncObjectStore + 'static> std::fmt::Debug
    for ActorsError<P, O>
{
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(error) => std::fmt::Debug::fmt(error, output),
            Self::Unknown(pending) => output
                .debug_tuple("Unknown")
                .field(pending.prepared())
                .finish(),
        }
    }
}

/// The original root publication, usable with the same [`super::serve`] entry.
pub struct PendingActors<P: AuthorityProvider + 'static, O: AsyncObjectStore + 'static>(
    hosting::Unsettled<P, O>,
);

impl<P: AuthorityProvider + 'static, O: AsyncObjectStore + 'static> PendingActors<P, O> {
    /// The original immutable request and its publication custody.
    #[must_use]
    pub fn prepared(&self) -> &hosting::Prepared {
        self.0.prepared()
    }
}

impl<P: AuthorityProvider + 'static, O: AsyncObjectStore + 'static> Composition
    for PendingActors<P, O>
{
    type Stream = P;
    type Objects = O;
    type Owner = Runtime;
    type Error = ActorsError<P, O>;

    fn open(
        self,
        _: &Arc<dyn Verifier>,
    ) -> BoxProviderFuture<'static, std::result::Result<Hosted<P, O, Runtime>, Self::Error>> {
        Box::pin(async move { started(self.0.resubmit().await.map_err(super::failed)?) })
    }
}

impl<P, O> Composition for Actors<P, O>
where
    P: AuthorityProvider + 'static,
    O: AsyncObjectStore + 'static,
{
    type Stream = P;
    type Objects = O;
    type Owner = Runtime;
    type Error = ActorsError<P, O>;

    fn open(
        self,
        _: &Arc<dyn Verifier>,
    ) -> BoxProviderFuture<'static, std::result::Result<Hosted<P, O, Runtime>, Self::Error>> {
        Box::pin(async move {
            let execution = self.execution;
            let tools = self
                .preset
                .tools(|_| execution.workspace, execution.tools)?;
            let context = match execution.context {
                Some(context) => context,
                None => Arc::new(Grants(self.preset.tool_grants(&tools)?)),
            };
            let executors = self.preset.executors(
                execution.model,
                tools,
                (execution.limits, execution.compaction),
                context,
            );
            let Placement {
                filesystem,
                provider,
                within,
            } = self.placement;
            let volumes = FilesystemHost::new(filesystem, &provider, None)?;
            let bound = Bound::admitted_host(
                self.issuer,
                self.caller,
                volumes,
                within,
                Turns {
                    executors: Arc::new(executors),
                    limits: execution.turns,
                    actors: execution.actors,
                    clock: Arc::new(SystemClock),
                    approval_policy: self.preset.approval_policy.clone(),
                },
                // No lazy checkout keeps fork cuts beside these volumes, so a
                // spawn forks each source's head.
                None,
                |turns| {
                    turns
                        .with_approvers(execution.approvers)
                        .with_goals(execution.goals)
                },
            )
            .map_err(super::failed)?;
            let serving = hosting::serve(bound, self.platform, &self.root.0, self.root.1)
                .await
                .map_err(super::failed)?;
            started(serving)
        })
    }
}

fn started<P: AuthorityProvider + 'static, O: AsyncObjectStore + 'static>(
    serving: Serving<P, O>,
) -> std::result::Result<Hosted<P, O, Runtime>, ActorsError<P, O>> {
    let server = match serving {
        Serving::Served(server) => server,
        Serving::Unknown(pending) => {
            return Err(ActorsError::Unknown(Box::new(PendingActors(pending))));
        }
    };
    let changes = server.bound().changes().map_err(super::failed)?;
    let runtime = Runtime::start(server.clone());
    let completion = runtime.completion();
    Ok(Hosted::new(
        server,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        runtime,
        Some(Arc::new(changes)),
    )
    .with_completion(completion))
}
