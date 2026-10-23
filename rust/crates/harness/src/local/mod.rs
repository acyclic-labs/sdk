//! Durable native defaults over the public filesystem and task runtime.

use crate::{
    Admission, AgentId, Capabilities, Error, OperationId, Result, TaskId,
    conversation::{
        ContentPublisher, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer, AuthorityVerifier, Scope},
    filesystem::{
        FilesystemContentPublisher, FilesystemHost, FilesystemResourceCreationProvider,
        FilesystemTaskRuntime, ForkPreparationMachine, StockTurnMachine,
    },
    model::{Model, ModelContent, ModelProvider},
    runtime::{RuntimeScope, RuntimeTask, TaskRegistry, TaskRunLimits},
    scheduler::SessionLimits,
    tool::{Tool, ToolRegistry},
    workflow::{MachineRegistry, WorkflowCommand},
};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_stream::LocalStream;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

mod admission;
mod bind;
#[cfg(feature = "native-execution")]
mod checkout_tools;
mod children;
mod command_machine;
mod command_views;
mod commands;
mod compact;
mod compaction;
mod config;
pub(crate) mod context;
mod files;
#[cfg(feature = "native-execution")]
pub mod host_files;
#[cfg(feature = "native-execution")]
pub mod host_owner;
#[cfg(feature = "native-execution")]
mod host_task;
#[cfg(feature = "native-execution")]
pub mod host_workspace;
mod inheritance;
mod input;
mod open;
mod orphans;
mod owner;
mod owner_record;
mod routing;
mod runtime;
#[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
mod settings;
#[cfg(test)]
mod tests;
mod turn;
#[cfg(feature = "search-http")]
pub mod web;
#[cfg(feature = "native-execution")]
mod workspace;
pub use config::LocalHarnessConfig;
use config::LocalProject;
#[cfg(feature = "native-execution")]
pub use workspace::LocalWorkspace;

/// Local durable filesystem host.
pub type LocalFilesystem<A = LocalAuthorityBackend, O = LocalObjectBackend> = FilesystemHost<A, O>;
/// Local durable task host and its execution journals.
pub type LocalTaskRuntime<P = LocalStream, A = LocalAuthorityBackend, O = LocalObjectBackend> =
    FilesystemTaskRuntime<P, A, O>;

/// A checkout or volume, durable authority, journals and stock executor.
pub struct LocalHarness {
    files: Arc<LocalFilesystem>,
    runtime: Arc<LocalTaskRuntime>,
    task_files: Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>>,
    project: VolumeRef,
    scratch: VolumeRef,
    journal: VolumeRef,
    verifier: AuthorityVerifier,
    issuer: Option<AuthorityIssuer>,
    signed: Scope,
    /// Absent for passive native-host coordination. No provider is constructed.
    agent: Option<(Model, Arc<dyn ModelProvider>)>,
    context_manifest: Option<crate::conversation::FileRef>,
    skill_cache: context::skills::Cache,
    #[cfg(feature = "hooks")]
    settings: Option<Arc<crate::native_settings::SettingsConfig>>,
    #[cfg(test)]
    context_read_observer:
        Option<for<'a> fn(&'a Self, &'a str) -> acyclic_stream::BoxProviderFuture<'a, Result<()>>>,
    limits: Limits,
    compaction: crate::context::CompactionPolicy,
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    mcp_diagnostics: Vec<crate::mcp::McpServerFailure>,
    goal_continuations: Option<crate::goal::GoalContinuationPolicy>,
    // Outermost lock: admission -> filesystem/provider locks. Never held during
    // model/tool execution, and provider callbacks never acquire admission.
    admission: crate::sync::owned::Arc<crate::sync::owned::Mutex<()>>,
    // Leaf lock owned by the one HEAD write in flight. That write outlives a
    // cancelled publisher, so every HEAD read waits for it first.
    publication: crate::sync::owned::Arc<crate::sync::owned::Mutex<()>>,
    // Leaf lock; claims and supersession hold `admission` first.
    drivers: orphans::Drivers,
    #[cfg(feature = "native-execution")]
    workspace: Option<Arc<workspace::LocalWorkspace>>,
    #[cfg(feature = "native-execution")]
    checkout_context: bool,
    #[cfg(all(test, feature = "native-execution"))]
    environment_reads: std::sync::atomic::AtomicUsize,
    #[cfg(feature = "native-execution")]
    clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    #[cfg(feature = "native-execution")]
    environment_platform: crate::native_environment::PlatformPolicy,
}

impl LocalHarness {
    fn agent(&self) -> Result<&(Model, Arc<dyn ModelProvider>)> {
        self.agent.as_ref().ok_or_else(|| {
            Error::Unsupported("native hosts own their agent loop and tool execution".into())
        })
    }
    #[cfg(feature = "hooks")]
    fn settings(&self) -> Result<&Arc<crate::native_settings::SettingsConfig>> {
        self.settings.as_ref().ok_or_else(|| {
            Error::Unsupported("native hosts own their settings and permissions".into())
        })
    }
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn task_stream(&self) -> Arc<LocalStream> {
        self.runtime.task_host().workflow_stream()
    }

    /// Opens durable local state and assembles the stock host before admitting work.
    pub fn open(
        config: LocalHarnessConfig,
    ) -> acyclic_stream::BoxProviderFuture<'static, Result<Self>> {
        Box::pin(open::open(config))
    }

    /// Trusted host's task-file volume; clients render its bounded live queries.
    pub fn task_files(&self) -> &Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>> {
        &self.task_files
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
    /// Host-only journal storage.
    #[must_use]
    pub const fn journal_volume(&self) -> &VolumeRef {
        &self.journal
    }
    /// Complete enabled tool registry.
    #[must_use]
    pub fn tools(&self) -> &ToolRegistry {
        self.runtime.tool_registry()
    }

    /// Optional configured servers that failed startup; required failures reject opening.
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    #[must_use]
    pub fn mcp_diagnostics(&self) -> &[crate::mcp::McpServerFailure] {
        &self.mcp_diagnostics
    }

    /// Canonical checkout path, absent for a configured volume.
    #[cfg(feature = "native-execution")]
    #[must_use]
    pub fn workspace_path(&self) -> Option<&Path> {
        self.workspace
            .as_ref()
            .map(|workspace| workspace.path.as_path())
    }
    /// Source-backed checkout view, absent for configured volumes.
    #[cfg(feature = "native-execution")]
    #[must_use]
    pub fn workspace(&self) -> Option<&Arc<LocalWorkspace>> {
        self.workspace.as_ref()
    }
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
