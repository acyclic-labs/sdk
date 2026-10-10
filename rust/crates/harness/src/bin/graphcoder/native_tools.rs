//! Native producer bindings for the shared, owner-fenced recursive tools.
//! Source capture, preparation, publication and admission remain library-owned.
#![allow(missing_docs, reason = "private producer-authenticated binary composition")]

use super::native::{AuthorityBinding, Config, Files, Runtime};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_harness::{
    Error, Result, TaskId,
    conversation::{ConversationMessage, VolumeRef},
    filesystem::{ConfiguredRecursiveAuthorityBinding, ConfiguredRecursiveCaller,
        ConfiguredRecursiveFork, ConfiguredRecursiveMerge, ConfiguredRecursiveToolSource,
        ConfiguredRecursiveToolsConfig, ForkPreparationLimits, RecursiveDefaultToolset},
    runtime::TaskAdmissionRecord,
    store::HistoryReadLimits,
    workflow::WorkflowCommand,
};
use acyclic_stream::{LocalStream, StreamClient};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub type Source = ConfiguredRecursiveToolSource<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

/// Explicit trusted producer bindings. Task, agent, effect and admission IDs
/// are separate. An unlisted model-chosen identity receives no authority.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeToolsConfig {
    #[serde(default)]
    pub callers: Vec<NativeToolCaller>,
    #[serde(default)]
    pub forks: Vec<NativeToolFork>,
    #[serde(default)]
    pub merges: Vec<NativeToolMerge>,
    /// Original finite producer allowance for cumulative checkpoint IO.
    pub content_budget: acyclic_fs::WorkBudget,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeToolCaller {
    pub task: TaskId,
    pub owner: AuthorityBinding,
    pub project: VolumeRef,
    pub private_volume: VolumeRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_snapshot: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_volume: Option<VolumeRef>,
}

/// An original signed allocation/input, not permission to mint a production
/// authority for an arbitrary tool ID. Its model command retains a pinned
/// payload FileRef; the shared owner verifies its exact text and read grant.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeToolFork {
    pub caller: TaskId,
    pub child: AuthorityBinding,
    pub project: VolumeRef,
    pub private_volume: VolumeRef,
    pub admission: TaskAdmissionRecord,
    pub limits: ForkPreparationLimits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_volume: Option<VolumeRef>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeToolMerge {
    pub caller: TaskId,
    pub source: TaskId,
    pub notice: ConversationMessage,
}

/// This adapter only translates trusted native configuration. The same source
/// object owns both tool registries and the runtime's preparation callback.
/// The shared source binds the original runtime once through Weak, avoiding a
/// source/registry/runtime ownership cycle and any descendant root fallback.
pub struct NativeToolSource {
    source: Arc<Source>,
}

impl NativeToolSource {
    /// Borrows configuration so unrelated original fork snapshots are not
    /// cloned. Only the finite provider bindings needed by the source are kept.
    pub fn new(config: &Config, stream: StreamClient<LocalStream>, files: Arc<Files>) -> Result<Arc<Self>> {
        if config.tools.callers.len() > 64 || config.tools.forks.len() > 64 || config.tools.merges.len() > 64 {
            return Err(Error::Invalid("native tools exceed the finite producer binding limit".into()));
        }
        let configured = ConfiguredRecursiveToolsConfig {
            callers: config.tools.callers.iter().map(|caller| ConfiguredRecursiveCaller {
                task: caller.task,
                owner: authority_binding(&caller.owner),
                project: caller.project.clone(),
                private_volume: caller.private_volume.clone(),
                native_snapshot: caller.native_snapshot.clone(),
                result_volume: caller.result_volume.clone(),
            }).collect(),
            forks: config.tools.forks.iter().map(|fork| {
                let command = WorkflowCommand::deserialize(&fork.admission.input)
                    .map_err(|error| Error::Invalid(format!("native child model command: {error}")))?;
                command.validate()?;
                if command.kind != acyclic_harness::filesystem::MODEL_TASK_COMMAND_KIND {
                    return Err(Error::Invalid("native child must retain the actual stock model command".into()));
                }
                Ok(ConfiguredRecursiveFork {
                    caller: fork.caller,
                    child: authority_binding(&fork.child),
                    project: fork.project.clone(),
                    private_volume: fork.private_volume.clone(),
                    admission: fork.admission.clone(),
                    // The model artifact contains untagged ModelContent::Text
                    // at /input; /payload selects that exact pinned artifact.
                    child_message_pointer: "/payload".into(),
                    child_message_file_pointer: Some("/input".into()),
                    input_files: vec![command.payload],
                    limits: fork.limits,
                    result_volume: fork.result_volume.clone(),
                })
            }).collect::<Result<Vec<_>>>()?,
            merges: config.tools.merges.iter().map(|merge| ConfiguredRecursiveMerge {
                caller: merge.caller,
                source: merge.source,
                notice: merge.notice.clone(),
            }).collect(),
            stream_provider: config.stream_provider.clone(),
            history_limits: HistoryReadLimits { maximum_events: 256, maximum_bytes: 4 * 1024 * 1024 },
            inherited_messages: 64,
            inherited_references: 64,
            content_budget: config.tools.content_budget,
            // Native assembly installs its actual service/graph policy
            // explicitly; this browser configuration hook cannot infer a root.
            project_policy_graph_root: None,
        };
        let source = Source::new(configured, stream, files, &config.filesystem_provider, config.limits)?;
        Ok(Arc::new(Self { source }))
    }

    pub fn tools(&self, maximum_request_bytes: u64) -> Result<RecursiveDefaultToolset> {
        self.source.tools(maximum_request_bytes)
    }

    /// Install this exact object with Runtime::with_recursive_source before
    /// constructing the runtime Arc and binding its original owner below.
    pub fn recursive_source(&self) -> Arc<Source> {
        self.source.clone()
    }

    pub fn bind_runtime(&self, runtime: &Arc<Runtime>) -> Result<()> {
        self.source.bind_runtime(runtime)
    }
}

fn authority_binding(binding: &AuthorityBinding) -> ConfiguredRecursiveAuthorityBinding {
    ConfiguredRecursiveAuthorityBinding {
        authority: binding.authority.clone(),
        issuer: binding.issuer.clone(),
        key: binding.key,
        scope: binding.scope.clone(),
    }
}
