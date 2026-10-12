//! Native producer bindings for the shared, owner-fenced recursive tools.
#![allow(missing_docs, reason = "private producer-authenticated binary composition")]

use super::native::{AuthorityBinding, Config, Files};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_harness::{
    Error, Result, TaskId,
    conversation::{ConversationMessage, VolumeRef},
    filesystem::{ConfiguredRecursiveAuthorityBinding, ConfiguredRecursiveCaller,
        ConfiguredRecursiveFork, ConfiguredRecursiveMerge, ConfiguredRecursiveSourceCheckpoint,
        ConfiguredRecursiveToolSource, ConfiguredRecursiveToolsConfig, ForkPreparationLimits,
    },
    runtime::TaskAdmissionRecord,
    store::HistoryReadLimits,
    workflow::WorkflowCommand,
};
use acyclic_stream::{LocalStream, StreamClient};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub type Source = ConfiguredRecursiveToolSource<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

/// Explicit trusted producer bindings.
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

/// An original signed allocation/input, not permission to mint a production authority for an arbitrary tool ID.
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

/// Builds the shared recursive source from trusted configuration.
pub fn source(config: &Config, stream: StreamClient<LocalStream>, files: Arc<Files>) -> Result<Arc<Source>> {
    if config.tools.callers.len() > 64 || config.tools.forks.len() > 64 || config.tools.merges.len() > 64
        || config.fork_sources.len() > 64
    {
        return Err(Error::Invalid("native tools exceed the finite producer binding limit".into()));
    }
    let configured = ConfiguredRecursiveToolsConfig {
        callers: config.tools.callers.iter().map(|caller| Ok(ConfiguredRecursiveCaller {
            task: caller.task,
            owner: authority_binding(&caller.owner)?,
            project: caller.project.clone(),
            private_volume: caller.private_volume.clone(),
            native_snapshot: caller.native_snapshot.clone(),
            result_volume: caller.result_volume.clone(),
        })).collect::<Result<Vec<_>>>()?,
        forks: config.tools.forks.iter().map(|fork| {
            let command = WorkflowCommand::deserialize(&fork.admission.input)
                .map_err(|error| Error::Invalid(format!("native child model command: {error}")))?;
            command.validate()?;
            if command.kind != acyclic_harness::filesystem::MODEL_TASK_COMMAND_KIND {
                return Err(Error::Invalid("native child must retain the actual stock model command".into()));
            }
            Ok(ConfiguredRecursiveFork {
                caller: fork.caller,
                child: authority_binding(&fork.child)?,
                project: fork.project.clone(),
                private_volume: fork.private_volume.clone(),
                admission: fork.admission.clone(),
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
        project_policy_graph_root: None,
        original_sources: config.fork_sources.iter()
            .filter(|source| config.tools.callers.iter().any(|caller| caller.task == source.caller))
            .map(|source| ConfiguredRecursiveSourceCheckpoint {
                caller: source.caller,
                request: source.request.clone(),
                snapshot: source.snapshot.clone(),
            }).collect(),
    };
    Source::new(configured, stream, files, &config.filesystem_provider, config.limits)
}

fn authority_binding(binding: &AuthorityBinding) -> Result<ConfiguredRecursiveAuthorityBinding> {
    ConfiguredRecursiveAuthorityBinding::new(
        acyclic_harness::core::AuthorityIssuer::new(&binding.issuer, binding.key, binding.authority.clone()),
        binding.scope.clone(),
    )
}
