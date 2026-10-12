//! Native producer bindings for the shared, owner-fenced recursive tools.
#![allow(
    missing_docs,
    reason = "private producer-authenticated binary composition"
)]

use super::native::{AuthorityBinding, Config, Files};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_harness::{
    Error, Result, TaskId,
    conversation::{ConversationMessage, VolumeRef},
    filesystem::{
        ConfiguredRecursiveAuthorityBinding, ConfiguredRecursiveCaller, ConfiguredRecursiveFork,
        ConfiguredRecursiveMerge, ConfiguredRecursiveToolSource, ConfiguredRecursiveToolsConfig,
        ForkPreparationLimits,
    },
    runtime::TaskAdmissionRecord,
    store::HistoryReadLimits,
    workflow::WorkflowCommand,
};
use acyclic_stream::LocalStream;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub type Source =
    ConfiguredRecursiveToolSource<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

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
pub fn source(
    config: &Config,
    stream: std::sync::Arc<LocalStream>,
    files: Arc<Files>,
) -> Result<Arc<Source>> {
    if config.tools.callers.len() > 64
        || config.tools.forks.len() > 64
        || config.tools.merges.len() > 64
        || config.fork_sources.len() > 64
        || config.tools.commands.len() > 64
    {
        return Err(Error::Invalid(
            "native tools exceed the finite producer binding limit".into(),
        ));
    }
    let configured = ConfiguredRecursiveToolsConfig {
        callers: config
            .tools
            .callers
            .iter()
            .map(|caller| ConfiguredRecursiveCaller {
                task: caller.task,
                owner: authority_binding(&caller.owner),
                project: caller.project.clone(),
                private_volume: caller.private_volume.clone(),
                native_snapshot: caller.native_snapshot.clone(),
                result_volume: caller.result_volume.clone(),
            })
            .collect(),
        forks: config
            .tools
            .forks
            .iter()
            .map(|fork| {
                let command =
                    WorkflowCommand::deserialize(&fork.admission.input).map_err(|error| {
                        Error::Invalid(format!("native child model command: {error}"))
                    })?;
                command.validate(&mut None)?;
                if command.kind != acyclic_harness::filesystem::MODEL_TASK_COMMAND_KIND {
                    return Err(Error::Invalid(
                        "native child must retain the actual stock model command".into(),
                    ));
                }
                Ok(ConfiguredRecursiveFork {
                    caller: fork.caller,
                    child: authority_binding(&fork.child),
                    project: fork.project.clone(),
                    private_volume: fork.private_volume.clone(),
                    admission: fork.admission.clone(),
                    child_message_pointer: "/payload".into(),
                    child_message_file_pointer: Some("/input".into()),
                    input_files: vec![command.payload],
                    limits: fork.limits,
                    result_volume: fork.result_volume.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?,
        merges: config
            .tools
            .merges
            .iter()
            .map(|merge| ConfiguredRecursiveMerge {
                caller: merge.caller,
                source: merge.source,
                notice: merge.notice.clone(),
            })
            .collect(),
        stream_provider: config.stream_provider.clone(),
        history_limits: HistoryReadLimits {
            maximum_events: 256,
            maximum_bytes: 4 * 1024 * 1024,
        },
        inherited_messages: 64,
        inherited_references: 64,
        content_budget: config.tools.content_budget,
        project_policy_graph_root: None,
    };
    Source::new(
        configured,
        stream,
        files,
        &config.filesystem_provider,
        config.limits,
    )
}

fn authority_binding(binding: &AuthorityBinding) -> ConfiguredRecursiveAuthorityBinding {
    ConfiguredRecursiveAuthorityBinding::new(
        acyclic_harness::core::AuthorityIssuer::new(
            &binding.issuer,
            binding.key,
            binding.authority.clone(),
        ),
        binding.scope.clone(),
    )
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
    pub async fn command(
        &self,
        caller: TaskId,
        lease: &acyclic_harness::distributed::WorkLease,
        operation: acyclic_harness::OperationId,
        decision: Option<bool>,
    ) -> Result<String> {
        use acyclic_harness::{
            conversation::ContentResidencyVerifier as _,
            core::{AuthorityIssuer, SchemaRegistry},
            effect_host::ConversationEffectHost,
            effects::EffectRegistry,
            filesystem::{
                BorrowedNativeVolumeBinding, FilesystemContentPublisher, FilesystemContentVerifier,
                FilesystemInteractionHost, NativeProcessAuthority, NativeProcessProvider,
                NativeProcessRequest, NativeVolumeView, PreparationIo,
            },
            scheduler::LeaseFence,
            store::history::HistoryIo as _,
        };
        let configured = self
            .config
            .tools
            .commands
            .iter()
            .find(|entry| entry.caller == caller && entry.operation == operation)
            .ok_or_else(|| {
                Error::Unauthorized("native command provider is not configured".into())
            })?;
        let context = self.caller_context(caller, lease).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            self.config.tools.content_budget,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let binding = self.caller_binding(caller).await?;
        let issuer = AuthorityIssuer::new(&binding.issuer, binding.key, binding.authority.clone());
        let verifier = issuer.verifier(&mut None)?;
        let fence = LeaseFence::from(&lease.reservation);
        let owner = self
            .runtime
            .task_host()
            .journal_owner_with_native_work(caller, &fence, &work)
            .await?
            .into_native_shared(&work)?;
        let reader = FilesystemContentVerifier::new(
            self.files.clone(),
            &verifier,
            &binding.scope,
            super::terminal::MAX_FRAME_BYTES as u64,
            Some(&io),
        )?;
        let result_volume = self
            .config
            .tools
            .callers
            .iter()
            .find(|entry| entry.task == caller)
            .and_then(|entry| entry.result_volume.as_ref())
            .ok_or_else(|| {
                Error::Unsupported("native command requires separate result storage".into())
            })?;
        let results = FilesystemContentPublisher::new(
            self.files.clone(),
            result_volume,
            &verifier,
            &binding.scope,
            super::terminal::MAX_FRAME_BYTES as u64,
            Some(&io),
        )?;
        let caller_private = &self
            .config
            .tools
            .callers
            .iter()
            .find(|entry| entry.task == caller)
            .ok_or_else(|| Error::NotFound("native caller private volume".into()))?
            .private_volume;
        let approvals = Arc::new(FilesystemInteractionHost::new(
            self.stream.clone(),
            self.files.clone(),
            &binding.authority,
            &verifier,
            &SchemaRegistry::default(),
            &binding.scope,
            caller_private,
            super::terminal::MAX_FRAME_BYTES as u64,
            Some(&io),
        )?);
        if let Some(approved) = decision {
            if !context.scope().grants().contains("interaction:route")
                || !binding.scope.capabilities().contains("interaction:resolve")
            {
                return Err(Error::Unauthorized(
                    "native command approval route is unavailable".into(),
                ));
            }
            let (ticket, _) = approvals
                .read(configured.approval, None)
                .await?
                .ok_or_else(|| Error::NotFound("native command approval".into()))?;
            let scope = issuer
                .issue(
                    "native-command-responder",
                    &acyclic_harness::Capabilities::new([
                        "interaction:resolve".to_owned(),
                        ticket.responder_grant(),
                    ]),
                    Some(
                        binding
                            .scope
                            .agent()
                            .ok_or_else(|| Error::Invalid("native command caller agent".into()))?,
                    ),
                    None,
                    &mut None,
                )
                .expect("scope proof");
            let receipt = approvals
                .resolve_approval(
                    acyclic_harness::OperationId::new(),
                    scope,
                    configured.approval,
                    1,
                    approved,
                    None,
                )
                .await?;
            return Ok(format!(
                "command-approval-json={}\n",
                serde_json::to_string(&receipt)
                    .map_err(|error| Error::Invalid(error.to_string()))?
            ));
        }
        let bytes = reader
            .read(
                &configured.plan.request,
                super::terminal::MAX_FRAME_BYTES as u64,
                Some(&io),
            )?
            .await?;
        let (request, _request_custody) =
            NativeProcessRequest::decode_with_native_work(&bytes, &work)?;
        let (allocation, _allocation_custody) = NativeVolumeView::<
            LocalAuthorityBackend,
            LocalObjectBackend,
        >::recover_allocation(
            &owner, &request.view.options, &work
        )?
        .await?;
        let allocation = allocation
            .ok_or_else(|| Error::NotFound("original native command view allocation".into()))?;
        let native_context = self
            .runtime
            .task_context_with_native_work(caller, &fence, &work)
            .await?;
        let mut guards = Vec::new();
        for _ in &request.view.volumes {
            guards.push(
                native_context
                    .volume_callback_guard_with_native_work(&owner, &work)
                    .await?,
            );
        }
        let volumes = request
            .view
            .volumes
            .iter()
            .map(|mapping| {
                let path = mapping
                    .path
                    .strip_prefix(&request.view.options.root)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .to_str()
                    .ok_or_else(|| Error::Invalid("native volume path encoding".into()))?;
                Ok(BorrowedNativeVolumeBinding {
                    host: self.files.as_ref(),
                    volume: &mapping.volume,
                    generation: &mapping.generation,
                    namespace: &mapping.namespace,
                    path,
                    writable: mapping.writable,
                    verifier: &verifier,
                    scope: &binding.scope,
                    publication: acyclic_fs::PublicationPermit::Unrestricted,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let existing_lazy = request
            .view
            .volumes
            .iter()
            .map(|_| None)
            .collect::<Vec<_>>();
        let view = NativeVolumeView::prepare(
            &owner,
            &guards,
            &allocation,
            &request.view.options,
            &volumes,
            &existing_lazy,
            &work,
        )?
        .await?;
        let mut process_io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(&work)?);
        let process_verifier = verifier.copy(&mut Some(&mut process_io))?;
        let authority = NativeProcessAuthority {
            verifier: process_verifier,
            history_limits: HistoryReadLimits {
                maximum_events: 256,
                maximum_bytes: 4 * 1024 * 1024,
            },
            content: reader.clone(),
            result_content: reader.clone(),
            results,
            approvals,
            authority_custody: Some(process_io.into_custody()?),
        };
        let provider = NativeProcessProvider::new(
            owner.clone(),
            operation,
            &configured.plan,
            view,
            configured.approval,
            authority,
            &work,
        )?
        .await?
        .into_arc_with_native_work(&work)?;
        let mut registry = EffectRegistry::new(Some(&work), None)?;
        registry.register(provider, Some(&work))?;
        let mut authority_io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(&work)?);
        let authority = authority_io.clone_authority(&binding.authority)?;
        let issuer = issuer.copy(&mut Some(&mut authority_io))?;
        let scope = binding.scope.copy(&mut Some(&mut authority_io))?;
        let effects = ConversationEffectHost::new(
            self.stream.clone(),
            authority,
            issuer,
            scope,
            SchemaRegistry::default(),
            reader,
            registry,
            Some(authority_io.into_custody()?),
            Some(&work),
        )?;
        let (status, _status_custody) = effects
            .run_task_effect(&owner, operation, &configured.plan, Some(&work))
            .await?;
        Ok(format!(
            "command-json={}\n",
            serde_json::to_string(&status).map_err(|error| Error::Invalid(error.to_string()))?
        ))
    }
}
