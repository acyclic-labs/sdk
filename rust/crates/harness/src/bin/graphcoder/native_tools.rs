//! Native producer bindings for the shared, owner-fenced recursive tools.
#![allow(missing_docs, reason = "private producer-authenticated binary composition")]

use super::native::{AuthorityBinding, Config, Files};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_harness::{
    Error, Result, TaskId,
    conversation::{ConversationMessage, VolumeRef},
    filesystem::{ConfiguredRecursiveAuthorityBinding, ConfiguredRecursiveCaller,
        ConfiguredRecursiveFork, ConfiguredRecursiveMerge, ConfiguredRecursiveSourceCheckpoint,
        ConfiguredRecursiveToolSource, ConfiguredRecursiveToolsConfig, ForkPreparationLimits},
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
    #[serde(default)]
    pub commands: Vec<NativeCommandConfig>,
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

/// Translate the trusted native configuration into the existing SDK source.
pub fn source(config: &Config, stream: StreamClient<LocalStream>, files: Arc<Files>) -> Result<Arc<Source>> {
    if config.tools.callers.len() > 64 || config.tools.forks.len() > 64 || config.tools.merges.len() > 64
        || config.fork_sources.len() > 64 || config.tools.commands.len() > 64
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
    let source = Source::new(configured, stream, files, &config.filesystem_provider, config.limits)?;
    Ok(Arc::new(source))
}

fn authority_binding(binding: &AuthorityBinding) -> Result<ConfiguredRecursiveAuthorityBinding> {
    ConfiguredRecursiveAuthorityBinding::new(acyclic_harness::core::AuthorityIssuer::new(
        &binding.issuer, binding.key, binding.authority.clone()), binding.scope.clone())
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCommandConfig {
    pub caller: TaskId,
    pub operation: acyclic_harness::OperationId,
    pub plan: acyclic_harness::effect_host::TaskEffectPlan,
    pub approval: acyclic_harness::InteractionId,
}

#[cfg(feature = "native-execution")]
impl super::native::NativeSession {
    pub async fn command(&self, caller: TaskId, lease: &acyclic_harness::distributed::WorkLease,
        operation: acyclic_harness::OperationId, decision: Option<bool>) -> Result<String> {
        use acyclic_harness::{conversation::ContentResidencyVerifier as _,
            core::{AuthorityIssuer, EffectStatus, SchemaRegistry},
            effect_host::ConversationEffectHost, effects::EffectRegistry,
            filesystem::{BorrowedNativeVolumeBinding, FilesystemContentPublisher, FilesystemContentVerifier,
                FilesystemInteractionHost, NativeProcessAuthority,
                NativeProcessProvider, NativeProcessRequest, NativeVolumeView},
            scheduler::LeaseFence};
        let configured = self.config.tools.commands.iter().find(|entry|
            entry.caller == caller && entry.operation == operation)
            .ok_or_else(|| Error::Unauthorized("native command provider is not configured".into()))?;
        self.caller_context(caller, lease).await?;
        let binding = self.caller_binding(caller).await?;
        let issuer = AuthorityIssuer::new(&binding.issuer, binding.key, binding.authority.clone());
        let verifier = issuer.verifier();
        let owner = Arc::new(self.runtime.task_host().journal_owner(caller,
            LeaseFence::from(&lease.reservation)).await?);
        let reader = Arc::new(FilesystemContentVerifier::new(self.files.clone(), verifier.clone(),
            binding.scope.clone(), super::terminal::MAX_FRAME_BYTES as u64)?);
        let result_volume = self.config.tools.callers.iter().find(|entry| entry.task == caller)
            .and_then(|entry| entry.result_volume.as_ref())
            .ok_or_else(|| Error::Unsupported("native command requires separate result storage".into()))?;
        let results = Arc::new(FilesystemContentPublisher::new(self.files.clone(), result_volume.clone(),
            &verifier, &binding.scope, super::terminal::MAX_FRAME_BYTES as u64)?);
        let caller_private = &self.config.tools.callers.iter().find(|entry| entry.task == caller)
            .ok_or_else(|| Error::NotFound("native caller private volume".into()))?.private_volume;
        let approvals = Arc::new(FilesystemInteractionHost::new(self.stream.clone(), self.files.clone(),
            binding.authority.clone(), verifier.clone(), SchemaRegistry::default(), binding.scope.clone(),
            caller_private.clone(), super::terminal::MAX_FRAME_BYTES as u64)?);
        if let Some(approved) = decision {
            let context = self.caller_context(caller, lease).await?;
            if !context.scope().grants().contains("interaction:route")
                || !binding.scope.capabilities().contains("interaction:resolve") {
                return Err(Error::Unauthorized("native command approval route is unavailable".into()));
            }
            let (ticket, _) = approvals.read(configured.approval).await?
                .ok_or_else(|| Error::NotFound("native command approval".into()))?;
            let scope = issuer.root_for_agent(binding.scope.agent()
                .ok_or_else(|| Error::Invalid("native command caller agent".into()))?,
                "native-command-responder", acyclic_harness::Capabilities::new([
                    "interaction:resolve".to_owned(), ticket.responder_grant()]));
            let receipt = approvals.resolve_approval(acyclic_harness::OperationId::new(), scope,
                configured.approval, 1, approved, None).await?;
            return Ok(format!("command-approval-json={}\n", serde_json::to_string(&receipt)
                .map_err(|error| Error::Invalid(error.to_string()))?));
        }
        let authority = || NativeProcessAuthority {
            verifier: verifier.clone(), history_limits: HistoryReadLimits {
                maximum_events: 256, maximum_bytes: 4 * 1024 * 1024 },
            content: reader.clone(), result_content: reader.clone(), results: results.clone(),
            approvals: approvals.clone(), authority_custody: None,
        };
        let recovered = Arc::new(NativeProcessProvider::recover(owner.clone(), operation,
            configured.plan.clone(), configured.approval, authority()).await?);
        let effects = |provider: Arc<NativeProcessProvider<LocalStream>>| -> Result<_> {
            let mut registry = EffectRegistry::new();
            registry.register(provider)?;
            Ok(ConversationEffectHost::new(self.stream.clone(), binding.authority.clone(),
                issuer.clone(), binding.scope.clone(), SchemaRegistry::default(), reader.clone(), registry)?)
        };
        let observed = match effects(recovered)?.reconcile_task_effect(&owner, operation, &configured.plan).await {
            Ok(status) => status,
            Err(Error::NotFound(_)) => EffectStatus::Planned,
            Err(error) => return Err(error),
        };
        let status = if observed == EffectStatus::Planned {
            let bytes = reader.read(&configured.plan.request).await?;
            let request: NativeProcessRequest = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let cancellation = acyclic_fs::CancellationToken::new();
            let allocation = NativeVolumeView::<LocalAuthorityBackend, LocalObjectBackend>::recover_allocation(
                &owner, &request.view.options, request.view.options.work_per_volume, &cancellation).await
                .map_err(|failure| failure.0)?.value
                .ok_or_else(|| Error::NotFound("original native command view allocation".into()))?;
            let paths = request.view.volumes.iter().map(|mapping| mapping.path
                .strip_prefix(&request.view.options.root)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .to_str().map(str::to_owned).ok_or_else(|| Error::Invalid("native volume path encoding".into())))
                .collect::<Result<Vec<_>>>()?;
            let volumes = request.view.volumes.iter().zip(&paths).map(|(mapping, path)|
                BorrowedNativeVolumeBinding {
                    host: self.files.as_ref(), volume: &mapping.volume, generation: &mapping.generation,
                    namespace: &mapping.namespace, path, writable: mapping.writable,
                    verifier: &verifier, scope: &binding.scope,
                    publication: acyclic_fs::PublicationPermit::Unrestricted,
                }).collect();
            let view = NativeVolumeView::prepare(&owner, allocation, request.view.options.clone(),
                volumes, &cancellation).await.map_err(|failure| failure.0)?.value;
            let provider = Arc::new(NativeProcessProvider::new(owner.clone(), operation, configured.plan.clone(),
                Arc::new(view), configured.approval, authority()).await?);
            effects(provider)?.run_task_effect(&owner, operation, configured.plan.clone()).await?
        } else { observed };
        Ok(format!("command-json={}\n", serde_json::to_string(&status)
            .map_err(|error| Error::Invalid(error.to_string()))?))
    }
}
