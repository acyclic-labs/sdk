//! Durable native defaults: the local filesystem and its agent host.

use crate::{
    AgentId, Capabilities, Error, OperationId, Result, TaskId,
    conversation::{
        ContentPublisher, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityRoot, AuthorityVerifier, Scope},
    filesystem::{FilesystemContentPublisher, FilesystemHost},
    model::{Model, ModelProvider},
    runtime::{AgentHarness, RuntimeScope, TaskRunLimits},
    tool::{Tool, ToolRegistry},
};
pub use acyclic_fs::native_host::read_regular_file;
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
pub use acyclic_harness_client::{AgentDelivery, EventDrain, EventProjection};
use acyclic_stream::LocalStream;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

// Unit fixtures exercise the native owner even for portable feature checks.
// Its providers are ordinary dev-dependencies; the library never depends on itself.
pub mod agent_conversation;
mod agent_settings;
mod agents;
#[cfg(any(test, not(target_arch = "wasm32")))]
mod checkout_tools;
mod config;
pub(crate) mod context;
pub mod continuation;
mod endpoint;
mod files;
#[cfg(not(target_arch = "wasm32"))]
pub mod host_files;
#[cfg(not(target_arch = "wasm32"))]
pub mod host_owner;
#[cfg(not(target_arch = "wasm32"))]
pub mod host_process;
#[cfg(not(target_arch = "wasm32"))]
pub mod host_workspace;
#[cfg(not(target_arch = "wasm32"))]
mod programs;
pub use endpoint::Connection;
mod observer;
pub(crate) mod open;
pub(crate) mod owner;
mod owner_record;
mod preflight;
#[cfg(not(target_arch = "wasm32"))]
mod serve;
#[cfg(not(target_arch = "wasm32"))]
pub mod service;
pub mod sessions;
#[cfg(any(test, not(target_arch = "wasm32")))]
mod settings;
pub mod study;
#[cfg(test)]
mod tests;
#[cfg(any(test, not(target_arch = "wasm32")))]
mod tool_metadata;
mod tool_output;
mod verifier;
pub use verifier::{Custody, KeyFileVerifier, Observation, RootCredential, Verifier};
#[cfg(not(target_arch = "wasm32"))]
pub mod web;
#[cfg(any(test, not(target_arch = "wasm32")))]
mod workspace;
pub use agents::{LocalAgents, LocalTurns};
use config::LocalProject;

/// The session volume shared by a local root and its descendants.
const SHARED_VOLUME: &str = "shared";
#[cfg(any(test, not(target_arch = "wasm32")))]
pub use config::ModelResolver;
pub use config::{LocalHarnessConfig, WorkspaceMounting};
pub use open::PreparedLocalHarness;

pub use observer::{Event, Observer};
pub use tool_output::LiveKind;
#[cfg(any(test, not(target_arch = "wasm32")))]
pub use workspace::LocalWorkspace;

/// Local durable filesystem host.
pub type LocalFilesystem<A = LocalAuthorityBackend, O = LocalObjectBackend> = FilesystemHost<A, O>;

/// A checkout or volume, durable authority, and the agents hosted on it.
pub struct LocalHarness {
    routing: Option<crate::agent::Routing>,
    peer_directory: Option<Arc<crate::hosting::PeerDirectory>>,
    topology: Option<crate::topology::Topology>,
    files: Arc<LocalFilesystem>,
    #[cfg(all(target_os = "linux", not(target_arch = "wasm32")))]
    volume_filesystem: acyclic_fs::LocalFs,
    /// Tools, scope and content bindings every agent turn's context starts from.
    harness: Arc<AgentHarness>,
    tools: ToolRegistry,
    /// The root agent's identity and signing key; its agents start on first
    /// use. An embedded `bind` has no signing custody, so it hosts no agents.
    root: AgentId,
    root_authority: Option<crate::core::AuthorityRoot>,
    /// The filesystem's volume-authority stream, which every agent runs on so
    /// that agents and their volumes share one consistency domain. An embedded
    /// `bind` has no filesystem of its own, so it hosts no agents.
    agent_stream: Option<Arc<LocalStream>>,
    agents: tokio::sync::OnceCell<agents::Hosted>,
    project: VolumeRef,
    scratch: VolumeRef,
    journal: VolumeRef,
    shared: VolumeRef,
    /// The read-only seed layer under every agent's home, at the generation
    /// this build wrote (see [`crate::seed`]).
    seed: (VolumeRef, crate::resources::GenerationRef),
    verifier: AuthorityVerifier,
    signed: Scope,
    /// The agents' shared approval policy.
    preset: crate::preset::Preset,
    /// The original host registry selection for tools bound to each turn.
    agent_tools: Vec<String>,
    question_plugin: bool,
    feedback_plugin: Option<bool>,
    approval_models: std::collections::BTreeMap<String, (Model, Arc<dyn ModelProvider>)>,
    /// Absent for passive native-host coordination.
    agent: Option<(Model, Arc<dyn ModelProvider>)>,
    models: Option<Arc<crate::model::ModelResolver>>,
    /// The sampling `agent.ts` selected for every model call.
    sampling: Option<crate::config::Sampling>,
    /// The verifier of a served tree's caller and clients; see
    /// [`LocalHarnessConfig::served`].
    served: Option<Arc<dyn Verifier>>,
    active_directory: String,
    #[cfg(any(test, not(target_arch = "wasm32")))]
    settings: Option<Arc<crate::native_settings::SettingsConfig>>,
    /// Plugins admitted at session start, including descriptions, after-hooks
    /// and before-hook rules ranked after the imported host hooks.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    plugin_set: Arc<crate::plugins::Enabled>,
    #[cfg(test)]
    context_read_observer:
        Option<for<'a> fn(&'a Self, &'a str) -> acyclic_stream::BoxProviderFuture<'a, Result<()>>>,
    limits: Limits,
    compaction: Option<crate::context::CompactionPolicy>,
    #[cfg(not(target_arch = "wasm32"))]
    mcp_diagnostics: Vec<crate::mcp::McpServerFailure>,
    goal_continuations: Option<crate::goal::GoalContinuationPolicy>,
    #[cfg(any(test, not(target_arch = "wasm32")))]
    workspace: Option<Arc<workspace::LocalWorkspace>>,
    #[cfg(any(test, not(target_arch = "wasm32")))]
    checkout_context: bool,
    /// What each agent was last told of its context epoch's sources.
    told: std::sync::Mutex<std::collections::HashMap<crate::agent::AgentPath, context::Told>>,
    clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    #[cfg(any(test, not(target_arch = "wasm32")))]
    environment_platform: crate::native_environment::PlatformPolicy,
}

impl LocalHarness {
    /// Initializes and pins the saved home before provider credentials bind.
    /// Starts neither configured MCP children nor agent execution.
    pub fn prepare(
        config: LocalHarnessConfig,
    ) -> acyclic_stream::BoxProviderFuture<'static, Result<PreparedLocalHarness>> {
        Box::pin(open::prepare(config))
    }
    #[cfg(any(test, not(target_arch = "wasm32")))]
    fn agent(&self) -> Result<&(Model, Arc<dyn ModelProvider>)> {
        self.agent.as_ref().ok_or_else(|| {
            Error::Unsupported("native hosts own their agent loop and tool execution".into())
        })
    }
    /// Opens durable local state and assembles the agent host's bindings.
    pub fn open(
        config: LocalHarnessConfig,
    ) -> acyclic_stream::BoxProviderFuture<'static, Result<Self>> {
        Box::pin(open::open(config))
    }

    /// Read-only configuration, workspace, mount and custody-policy checks.
    /// Never creates state or authority, captures a checkout or starts MCP
    /// children. Success does not open storage or probe a credential service.
    pub fn doctor(
        config: LocalHarnessConfig,
    ) -> acyclic_stream::BoxProviderFuture<'static, Vec<Error>> {
        Box::pin(open::doctor(config))
    }

    /// Durable content and project host.
    #[must_use]
    pub const fn filesystem(&self) -> &Arc<LocalFilesystem> {
        &self.files
    }
    /// Selected project view.
    #[must_use]
    pub const fn project(&self) -> &VolumeRef {
        &self.project
    }
    /// Agent-owned scratch content, distinct from host journal storage.
    #[must_use]
    pub const fn scratch(&self) -> &VolumeRef {
        &self.scratch
    }
    /// The root's workspace-private volume (the legacy field is named journal).
    /// Protected execution journals are separate, owner-bound volumes.
    #[must_use]
    pub const fn journal_volume(&self) -> &VolumeRef {
        &self.journal
    }
    /// Complete enabled tool registry.
    #[must_use]
    pub fn tools(&self) -> &ToolRegistry {
        &self.tools
    }

    /// Optional configured servers that failed startup; required failures reject opening.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn mcp_diagnostics(&self) -> &[crate::mcp::McpServerFailure] {
        &self.mcp_diagnostics
    }

    /// Canonical checkout path, absent for a configured volume.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    #[must_use]
    pub fn workspace_path(&self) -> Option<&Path> {
        self.workspace
            .as_ref()
            .map(|workspace| workspace.path.as_path())
    }
    /// Source-backed checkout view, absent for configured volumes.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    #[must_use]
    pub fn workspace(&self) -> Option<&Arc<LocalWorkspace>> {
        self.workspace.as_ref()
    }

    /// Releases this host's native mounts while preserving its durable state.
    /// Consuming the host excludes live borrows; callers must first join
    /// active agent turns and drop their observations.
    pub async fn disconnect(self) -> Result<()> {
        #[cfg(any(test, not(target_arch = "wasm32")))]
        let retired = if let Some(workspace) = &self.workspace {
            workspace.shutdown.cancel();
            workspace::volumes::retire(&workspace.mounts).await
        } else {
            Ok(())
        };
        #[cfg(not(any(test, not(target_arch = "wasm32"))))]
        let retired = Ok(());
        // Native view destructors join callbacks. They must not occupy the
        // coordination thread that those callbacks may still need to finish.
        acyclic_native_runtime::spawn_blocking(move || drop(self))
            .await
            .map_err(storage)?;
        retired
    }
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
