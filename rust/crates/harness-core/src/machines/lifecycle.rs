//! An agent's own Machines on its tree's provisioned targets.
//!
//! The target table is the tree's (every agent reads the root's `targets`),
//! but each agent runs a provisioned target on a Machine of its own, bound by
//! one v1 [`MachineBinding`] at [`AgentPath::machine`]. No child inherits
//! that binding, so the lifecycle follows the agent and never a timer:
//!
//! - **First exec** creates the agent's Machine, or forks its parent's when
//!   the parent has one whose contract offers a live or disk fork. Spawning
//!   costs nothing; an agent that never names the target never gets one.
//! - **Idle** is the provider's: every Machine is created under the default
//!   [`acyclic_machines::SuspensionPolicy::AfterIdle`], so the provider
//!   suspends it and the next exec wakes it inside its own deadline.
//! - **Pause** suspends the agent's Machines; its next exec wakes them.
//! - **Archive** destroys them before the step that closes the agent commits.
//!
//! Every provider mutation carries a key derived from (agent, target), so a
//! retried first use names the same Machine and a retried archive replays
//! the original destruction instead of repeating it.

use super::*;
use crate::agent::AgentPath;
use crate::tool::{ToolExecutor, ToolInvocation, ToolResult};
use acyclic_machines::MachineState;
use acyclic_stream::{
    AppendRecord, BoxProviderFuture, CommitCondition, CommitMutation, CommitRequest, ReadRequest,
    StreamError, StreamProviderExt as _,
};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

/// The persisted format of [`MachineBinding`].
pub const MACHINE_BINDING_VERSION: u32 = 1;

/// One agent's own Machine on one provisioned target (v1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineBinding {
    /// Always [`MACHINE_BINDING_VERSION`].
    pub version: u32,
    /// The provisioned target's name.
    pub target: String,
    /// The Machine, as its canonical id.
    pub machine: String,
    /// The parent's Machine it was forked from; `None` when created.
    pub forked_from: Option<String>,
}

/// What a provisioned target's new Machines are created from.
#[derive(Clone)]
pub struct ProvisionedTarget {
    /// The target's name in the tree's roster.
    pub name: String,
    /// The provider that owns its Machines.
    pub host: MachinesHost,
    /// The image, or the provider default.
    pub artifact: Option<ArtifactRef>,
    /// The network policy every Machine of the target commits to.
    pub network_policy_digest: [u8; 32],
}

/// Runs the commands of one agent's Machine once it exists: the host binds
/// its authority (for example a [`super::MachinesWorkspace`]) to the Machine.
pub trait MachineExecutors: acyclic_stream::ProviderPlatform {
    /// The executor of `sandbox`, observed as `observed`.
    ///
    /// # Errors
    /// When the host refuses to bind it.
    fn bind(
        &self,
        target: &ProvisionedTarget,
        sandbox: SandboxRef,
        observed: MachineObservation,
    ) -> Result<Arc<dyn ToolExecutor>>;
}

/// Every agent's Machines on the provisioned targets, over the stream
/// provider that holds the agents' subtrees.
pub struct AgentMachines<P: ?Sized> {
    stream: Arc<P>,
    targets: Vec<ProvisionedTarget>,
}

impl<P: crate::store::JournalProvider + ?Sized> AgentMachines<P> {
    /// The lifecycle of `targets`, whose bindings live in `stream`.
    ///
    /// # Errors
    /// When a target name repeats.
    pub fn new(stream: Arc<P>, targets: Vec<ProvisionedTarget>) -> Result<Self> {
        let mut names = std::collections::BTreeSet::new();
        if !targets
            .iter()
            .all(|target| names.insert(target.name.as_str()))
        {
            return Err(Error::Invalid(
                "provisioned target names must be unique".into(),
            ));
        }
        Ok(Self { stream, targets })
    }

    /// The provisioned targets.
    #[must_use]
    pub fn targets(&self) -> &[ProvisionedTarget] {
        &self.targets
    }

    fn target(&self, name: &str) -> Result<&ProvisionedTarget> {
        self.targets
            .iter()
            .find(|target| target.name == name)
            .ok_or_else(|| Error::Invalid(format!("'{name}' is not a provisioned target")))
    }

    /// `agent`'s binding on `target`, `None` before its first use.
    ///
    /// # Errors
    /// When the binding cannot be read or is not a v1 binding of `target`.
    pub async fn binding(&self, agent: &AgentPath, target: &str) -> Result<Option<MachineBinding>> {
        let path = agent.machine(target).map_err(agent_error)?;
        let request = ReadRequest {
            key: None,
            path,
            from: 0,
            limit: 1,
            at_least: Default::default(),
        };
        let mut records = match self.stream.read(request).await {
            Ok(records) => records,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let Some(record) = records.try_next().await? else {
            return Ok(None);
        };
        let binding: MachineBinding = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(format!("Machine binding does not decode: {error}")))?;
        if binding.version != MACHINE_BINDING_VERSION || binding.target != target {
            return Err(Error::Storage(
                "Machine binding is not a v1 binding of its target".into(),
            ));
        }
        Ok(Some(binding))
    }

    /// `agent`'s Machine on `target`, created on this first use: a fork of
    /// its parent's Machine when the parent has one that can fork, otherwise
    /// a new Machine. Later calls read the binding and touch no provider.
    ///
    /// # Errors
    /// When the provider refuses, or the outcome is uncertain: retrying the
    /// call names the same Machine.
    pub async fn machine(&self, agent: &AgentPath, target: &str) -> Result<SandboxRef> {
        let provisioned = self.target(target)?;
        if let Some(binding) = self.binding(agent, target).await? {
            return sandbox(provisioned, &binding);
        }
        let parent = match agent.parent() {
            Some(parent) => self.binding(&parent, target).await?,
            None => None,
        };
        let binding = match parent {
            Some(parent) => self.fork(provisioned, agent, &parent).await?,
            None => None,
        };
        let binding = match binding {
            Some(binding) => binding,
            None => self.create(provisioned, agent).await?,
        };
        self.bind(agent, binding).await
    }

    async fn fork(
        &self,
        target: &ProvisionedTarget,
        agent: &AgentPath,
        parent: &MachineBinding,
    ) -> Result<Option<MachineBinding>> {
        let source = sandbox(target, parent)?;
        let observed = target.host.inspect(&source).await?;
        if observed.contract.fork_fidelity().is_none() {
            return Ok(None);
        }
        // A suspended source (paused, or idled by the provider) is its own
        // checkpoint: it forks without waking.
        if !matches!(
            observed.state,
            MachineState::Running | MachineState::Suspended
        ) {
            return Ok(None);
        }
        let (operation, key) = intent("fork", agent, &target.name)?;
        let (_, children) = target
            .host
            .fork_machine(operation, &key, &source, std::num::NonZeroU32::MIN)
            .await?;
        let child = children
            .into_iter()
            .next()
            .ok_or_else(|| Error::Storage("Machines forked no child".into()))?;
        Ok(Some(MachineBinding {
            version: MACHINE_BINDING_VERSION,
            target: target.name.clone(),
            machine: target.host.machine_id(&child)?.to_string(),
            forked_from: Some(parent.machine.clone()),
        }))
    }

    async fn create(
        &self,
        target: &ProvisionedTarget,
        agent: &AgentPath,
    ) -> Result<MachineBinding> {
        let (operation, key) = intent("create", agent, &target.name)?;
        let created = target
            .host
            .create(
                operation,
                &key,
                target.artifact.as_ref(),
                None,
                target.network_policy_digest,
                |_| {},
            )
            .await?;
        Ok(MachineBinding {
            version: MACHINE_BINDING_VERSION,
            target: target.name.clone(),
            machine: target.host.machine_id(&created)?.to_string(),
            forked_from: None,
        })
    }

    /// Commits `binding` once; a concurrent first use committed the same one.
    async fn bind(&self, agent: &AgentPath, binding: MachineBinding) -> Result<SandboxRef> {
        let target = self.target(&binding.target)?;
        let path = agent.machine(&binding.target).map_err(agent_error)?;
        let value = serde_json::to_vec(&binding)
            .map_err(|error| Error::Invalid(format!("Machine binding encode: {error}")))?;
        let (_, key) = intent("bind", agent, &binding.target)?;
        let request = CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![CommitCondition::Absent { path: path.clone() }],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![AppendRecord::from(value)],
            }],
            // Agent paths are long; the commit key is their digest.
            idempotency_key: acyclic_stream::IdempotencyKey::new(
                blake3::hash(key.as_str().as_bytes()).as_bytes().to_vec(),
            )?,
        };
        // An undecided commit stays a storage failure: the caller retries
        // the first use, which names the same Machine and the same binding.
        match crate::agent::submit(&*self.stream, request)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
        {
            crate::agent::Submitted::Committed(_) => sandbox(target, &binding),
            _ => match self.binding(agent, &binding.target).await? {
                Some(winner) if winner.machine == binding.machine => sandbox(target, &winner),
                _ => Err(Error::Storage(
                    "a concurrent first use bound another Machine".into(),
                )),
            },
        }
    }

    /// Suspends every Machine `agent` holds: what a paused agent keeps.
    /// Its next exec wakes each one.
    ///
    /// # Errors
    /// When a provider refuses or its outcome is uncertain; suspending again
    /// is harmless.
    pub async fn suspend(&self, agent: &AgentPath) -> Result<()> {
        let pause = OperationId::new();
        for target in &self.targets {
            let Some(binding) = self.binding(agent, &target.name).await? else {
                continue;
            };
            let key = IdempotencyKey::new(format!(
                "acyclic.harness.machine.suspend.v1:{agent}:{}:{pause}",
                target.name
            ))?;
            target
                .host
                .suspend(pause, &key, &sandbox(target, &binding)?)
                .await?;
        }
        Ok(())
    }

    /// Destroys every Machine `agent` holds, each under the key of
    /// (agent, target): a retry replays the original destruction.
    ///
    /// A provider refuses to destroy a fork source while its forks live
    /// ([`acyclic_machines::Precondition::LiveForks`]).
    /// An archive closes a parent before its children, so a refused source
    /// stays until its last fork closes: each closing fork then destroys its
    /// closed ancestors' sources, under their own keys, as far as the
    /// provider admits. Whichever fork closes last finds no live sibling.
    ///
    /// # Errors
    /// When a provider refuses otherwise or its outcome is uncertain.
    pub async fn destroy(&self, agent: &AgentPath) -> Result<()> {
        for target in &self.targets {
            let Some(mut binding) = self.binding(agent, &target.name).await? else {
                continue;
            };
            let mut holder = agent.clone();
            loop {
                if !self.release(target, &holder, &binding).await? {
                    break;
                }
                let Some(parent) = holder.parent() else {
                    break;
                };
                let source = match &binding.forked_from {
                    Some(source) if self.closed(&parent).await? => source.clone(),
                    _ => break,
                };
                match self.binding(&parent, &target.name).await? {
                    Some(held) if held.machine == source => binding = held,
                    _ => break,
                }
                holder = parent;
            }
        }
        Ok(())
    }

    /// Destroys `holder`'s Machine; `false` while the provider keeps it for
    /// its live forks.
    async fn release(
        &self,
        target: &ProvisionedTarget,
        holder: &AgentPath,
        binding: &MachineBinding,
    ) -> Result<bool> {
        let (operation, key) = intent("destroy", holder, &target.name)?;
        match target
            .host
            .destroy(operation, &key, &sandbox(target, binding)?)
            .await
        {
            Ok(()) => Ok(true),
            Err(Error::Machines(acyclic_machines::ProviderError::FailedPrecondition(
                acyclic_machines::Precondition::LiveForks,
            ))) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn closed(&self, agent: &AgentPath) -> Result<bool> {
        match self.stream.tail(agent.closed().map_err(agent_error)?).await {
            Ok(_) => Ok(true),
            Err(StreamError::NotFound) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

impl<P: crate::store::JournalProvider + ?Sized + 'static> AgentMachines<P> {
    /// The executor of `agent`'s commands on `target`: its Machine is made on
    /// the first command, then `executors` binds it once for the agent.
    ///
    /// # Errors
    /// When `target` is not provisioned.
    pub fn executor(
        self: &Arc<Self>,
        agent: AgentPath,
        target: &str,
        executors: Arc<dyn MachineExecutors>,
    ) -> Result<Arc<dyn ToolExecutor>> {
        self.target(target)?;
        Ok(Arc::new(OnFirstUse {
            machines: Arc::clone(self),
            agent,
            target: target.to_owned(),
            executors,
            bound: OnceLock::new(),
        }))
    }
}

impl<P: crate::store::JournalProvider + ?Sized + 'static> crate::agent::AgentLifecycle
    for AgentMachines<P>
{
    fn pause<'a>(
        &'a self,
        agent: &'a AgentPath,
    ) -> BoxProviderFuture<'a, std::result::Result<(), crate::agent::Error>> {
        Box::pin(async move {
            self.suspend(agent)
                .await
                .map_err(|error| crate::agent::Error::Turn(Box::new(error)))
        })
    }

    fn close<'a>(
        &'a self,
        agent: &'a AgentPath,
    ) -> BoxProviderFuture<'a, std::result::Result<(), crate::agent::Error>> {
        Box::pin(async move {
            self.destroy(agent)
                .await
                .map_err(|error| crate::agent::Error::Turn(Box::new(error)))
        })
    }
}

/// One agent's executor on one provisioned target, bound on first use.
struct OnFirstUse<P: ?Sized> {
    machines: Arc<AgentMachines<P>>,
    agent: AgentPath,
    target: String,
    executors: Arc<dyn MachineExecutors>,
    bound: OnceLock<Arc<dyn ToolExecutor>>,
}

impl<P: crate::store::JournalProvider + ?Sized + 'static> OnFirstUse<P> {
    async fn executor(&self) -> Result<Arc<dyn ToolExecutor>> {
        if let Some(bound) = self.bound.get() {
            return Ok(Arc::clone(bound));
        }
        let target = self.machines.target(&self.target)?;
        let sandbox = self.machines.machine(&self.agent, &self.target).await?;
        self.bind(target, sandbox).await
    }

    async fn bind(
        &self,
        target: &ProvisionedTarget,
        sandbox: SandboxRef,
    ) -> Result<Arc<dyn ToolExecutor>> {
        let observed = target.host.inspect(&sandbox).await?;
        let executor = self.executors.bind(target, sandbox, observed)?;
        Ok(Arc::clone(self.bound.get_or_init(|| executor)))
    }
}

impl<P: crate::store::JournalProvider + ?Sized + 'static> ToolExecutor for OnFirstUse<P> {
    fn authorize(
        &self,
        scope: Option<&crate::runtime::RuntimeScope>,
        context: Option<&crate::runtime::ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        if let Some(bound) = self.bound.get() {
            return bound.authorize(scope, context, invocation, work);
        }
        // Before its first use the Machine's executor authorizes inside
        // `execute`, ahead of any command; a Machine runs no native work.
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "machine commands require ordinary task execution",
            });
        }
        let _ = (scope, context, invocation, work);
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<crate::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let executor = self.executor().await?;
            executor.authorize(None, context.as_ref(), &invocation, None)?;
            executor.execute(invocation, context).await
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<crate::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let executor = match self.bound.get() {
                Some(bound) => Arc::clone(bound),
                None => {
                    // Recovery observes the original binding; it cannot create
                    // a replacement Machine for an interrupted command.
                    let binding = self
                        .machines
                        .binding(&self.agent, &self.target)
                        .await?
                        .ok_or(Error::Indeterminate(invocation.operation_id))?;
                    let target = self.machines.target(&self.target)?;
                    self.bind(target, sandbox(target, &binding)?).await?
                }
            };
            executor.authorize(None, context.as_ref(), &invocation, None)?;
            executor.reconcile(invocation, context).await
        })
    }
}

fn sandbox(target: &ProvisionedTarget, binding: &MachineBinding) -> Result<SandboxRef> {
    let id =
        MachineId::parse(&binding.machine).map_err(|error| Error::Invalid(error.to_string()))?;
    SandboxRef::new(target.host.provider_ref.clone(), id.as_bytes().into(), None)
}

/// The operation and key of `verb` for (agent, target): the same on every retry.
fn intent(verb: &str, agent: &AgentPath, target: &str) -> Result<(OperationId, IdempotencyKey)> {
    let key = IdempotencyKey::new(format!(
        "acyclic.harness.machine.{verb}.v1:{agent}:{target}"
    ))?;
    let digest = blake3::hash(key.as_str().as_bytes());
    let operation = digest
        .as_bytes()
        .first_chunk::<16>()
        .ok_or_else(|| Error::Invalid("key digest".into()))?;
    Ok((OperationId::from_bytes(*operation), key))
}

fn agent_error(error: crate::agent::Error) -> Error {
    Error::Invalid(error.to_string())
}
