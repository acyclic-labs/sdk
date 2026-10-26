//! The stock harness code artifact, shared by local and deployed hosts.

use super::{ActorPackage, Bindings, Manifest};
use crate::{
    Result,
    filesystem::{ChildAgentMachine, ForkPreparationMachine, StockTurnMachine},
    runtime::TaskRegistry,
    workflow::{MachineIdentity, MachineRegistry, ResumableMachine},
};
use std::sync::Arc;

mod command_machine;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod subscription;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod subscription_claim;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub(crate) mod worker;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub use subscription::CommandSubscription;
#[cfg(all(test, feature = "filesystem-local", not(target_arch = "wasm32")))]
pub(crate) use subscription::execute_request;
#[cfg(all(test, feature = "filesystem-local"))]
mod tests;

/// Immutable code and task schemas. Resource bindings belong to installation,
/// so constructing code never discovers credentials, providers or host paths.
pub struct HarnessCode {
    tasks: TaskRegistry,
    machines: Vec<Arc<dyn ResumableMachine>>,
}

impl HarnessCode {
    /// Installs this exact code bundle over the selected local providers. All
    /// stock tasks, including direct tools and children, come from this bundle.
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    pub fn open_local(
        self,
        config: crate::local::LocalHarnessConfig,
    ) -> acyclic_stream::BoxProviderFuture<'static, Result<crate::local::LocalHarness>> {
        Box::pin(crate::local::open::open(self, config))
    }

    /// Builds every stock handler and its typed admission schema together.
    pub fn new() -> Result<Self> {
        Self::build(false)
    }

    /// Uses the same compiled actor bundle for passive native-host coordination.
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    pub(crate) fn native_host() -> Result<Self> {
        Self::build(true)
    }

    fn build(native: bool) -> Result<Self> {
        let stock = Arc::new(StockTurnMachine::new());
        let child = Arc::new(ChildAgentMachine::new());
        let fork = Arc::new(ForkPreparationMachine::new());
        let direct = Arc::new(command_machine::DirectTool::new());
        let mut tasks = TaskRegistry::default();
        #[cfg(all(
            feature = "filesystem-local",
            feature = "native-execution",
            not(target_arch = "wasm32")
        ))]
        if native {
            let host = crate::local::host_task::NativeHostTask::new();
            tasks.register(host.definition()?)?;
            tasks.register(fork.definition::<crate::resource_preparation::ResourcePreparationRequest, crate::fork::ForkReport>()?)?;
            return Ok(Self {
                tasks,
                machines: vec![host, fork],
            });
        }
        #[cfg(not(all(
            feature = "filesystem-local",
            feature = "native-execution",
            not(target_arch = "wasm32")
        )))]
        if native {
            return Err(crate::Error::Unsupported(
                "native hosts require native-execution".into(),
            ));
        }
        tasks.register(stock.definition()?)?;
        tasks.register(child.definition()?)?;
        tasks.register(fork.definition::<
            crate::resource_preparation::ResourcePreparationRequest,
            crate::fork::ForkReport,
        >()?)?;
        tasks.register(direct.definition()?)?;
        Ok(Self {
            tasks,
            machines: vec![stock, child, fork, direct],
        })
    }

    /// Typed task admissions for these exact pinned implementations.
    #[must_use]
    pub const fn tasks(&self) -> &TaskRegistry {
        &self.tasks
    }

    /// Exact actor identities, shared by package metadata and task admission.
    pub fn actors(&self) -> impl Iterator<Item = &MachineIdentity> {
        self.machines.iter().map(|machine| machine.identity())
    }

    /// Resolves deterministic implementations for the existing workflow engine.
    pub fn registry(&self) -> Result<MachineRegistry> {
        let mut registry = MachineRegistry::default();
        for machine in &self.machines {
            registry.register(machine.clone(), &mut None)?;
        }
        Ok(registry)
    }

    /// Binds the same immutable code for one runtime installation.
    pub fn bind(self, bindings: Bindings) -> Result<HarnessPackage> {
        use crate::filesystem::{
            EFFECT_TASK_COMMAND_KIND, FORK_PREPARE_TASK_COMMAND_KIND,
            MAIL_RECEIVE_TASK_COMMAND_KIND, MAIL_SEND_TASK_COMMAND_KIND, MODEL_TASK_COMMAND_KIND,
            TASK_ADMIT_COMMAND_KIND, TASK_ADMIT_PREPARED_COMMAND_KIND, TASK_CANCEL_COMMAND_KIND,
            TASK_OBSERVE_COMMAND_KIND, TIMER_TASK_COMMAND_KIND, TOOL_TASK_COMMAND_KIND,
        };
        let manifest = Manifest {
            actors: self.actors().cloned().collect(),
            effects: [
                MODEL_TASK_COMMAND_KIND,
                TOOL_TASK_COMMAND_KIND,
                EFFECT_TASK_COMMAND_KIND,
                TIMER_TASK_COMMAND_KIND,
                MAIL_SEND_TASK_COMMAND_KIND,
                MAIL_RECEIVE_TASK_COMMAND_KIND,
                TASK_ADMIT_COMMAND_KIND,
                TASK_OBSERVE_COMMAND_KIND,
                TASK_CANCEL_COMMAND_KIND,
                FORK_PREPARE_TASK_COMMAND_KIND,
                TASK_ADMIT_PREPARED_COMMAND_KIND,
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            bindings,
        };
        manifest.validate()?;
        Ok(HarnessPackage {
            manifest,
            code: self,
        })
    }
}

/// One versioned harness artifact with explicit platform resources.
pub struct HarnessPackage {
    manifest: Manifest,
    code: HarnessCode,
}

impl ActorPackage for HarnessPackage {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    fn machines(&self) -> Vec<Arc<dyn ResumableMachine>> {
        self.code.machines.clone()
    }
}
