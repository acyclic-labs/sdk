use super::*;
use acyclic_harness::filesystem::PreparationIo;
use acyclic_harness::store::HistoryIo;

/// Immutable launch content read by its admitted child.
#[derive(Clone, PartialEq, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeLaunchIntent {
    pub(crate) agent_alias: String,
    pub(crate) child_session_id: String,
    pub(crate) source: PiNativeSourceCut,
    pub(crate) invocation: PiNativeInvocation,
    pub(crate) session_path: String,
    pub(crate) session_bytes: String,
    pub(crate) maximum_entries: u32,
    pub(crate) maximum_bytes: u64,
}

impl PiNativeLaunchIntent {
    pub(crate) async fn load(
        context: &TaskContext,
        reference: &FileRef,
        work: &NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        if context.durable_task_id().is_none()
            || reference.descriptor().media_type() != "application/json"
        {
            return Err(invalid(
                "Pi launch input requires its actual durable child content binding",
            ));
        }
        let bytes = context
            .read_file(reference, Some(&PreparationIo::from_native_work(work)?))
            .await?;
        let maximum = context
            .scope()
            .limits()
            .file_bytes
            .min(crate::MAXIMUM_ADAPTER_STATE_BYTES);
        let intent = crate::pi_root_provider::guarded_pi_decode::<Self>(
            &bytes,
            maximum,
            acyclic_harness::wire::ErrorCode::Invalid,
            JsonValueSchema::None,
            PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        intent.0.validate(work)?;
        Ok(intent)
    }

    pub(crate) fn validate(&self, work: &NativeMaterializationWork) -> Result<()> {
        if self.agent_alias.is_empty()
            || self.agent_alias.len() > 128
            || !self
                .agent_alias
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        {
            return Err(invalid(
                "Pi original child alias must match [A-Za-z0-9_.-]{1,128}",
            ));
        }
        let path = Path::new(&self.session_path);
        if self.maximum_bytes > crate::MAXIMUM_ADAPTER_STATE_BYTES
            || self.session_path.is_empty()
            || path.is_absolute()
            || path
                .components()
                .any(|part| !matches!(part, Component::Normal(_)))
        {
            return Err(invalid(
                "Pi session path or original context allowance is invalid",
            ));
        }
        self.source.validate_session(
            &self.child_session_id,
            &self.invocation.cwd,
            self.session_bytes.as_bytes(),
            self.maximum_entries,
            self.maximum_bytes,
            work,
        )
    }

    pub(crate) fn request_with_work(
        &self,
        runtime: &PiNativeHostRuntime,
        binding: &PiTaskBinding,
        manifest: &NativeViewManifest,
        work: &NativeMaterializationWork,
    ) -> Result<(NativeProcessRequest, acyclic_fs::NativeAllocation)> {
        self.validate(work)?;
        let scratch = manifest
            .volumes
            .iter()
            .find(|mapping| mapping.volume == binding.scratch.volume)
            .ok_or_else(|| invalid("Pi launch has no original scratch mapping"))?;
        if self
            .invocation
            .session_file
            .strip_prefix(&scratch.path)
            .ok()
            != Some(Path::new(&self.session_path))
        {
            return Err(invalid(
                "Pi staged native session differs from the approved CLI session leaf",
            ));
        }
        self.invocation
            .request_with_work(runtime, binding, manifest, work)
    }

    pub(crate) async fn publish_session(
        &self,
        context: &TaskContext,
        operation: OperationId,
        work: &NativeMaterializationWork,
    ) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
        self.validate(work)?;
        if context.durable_task_id().is_none() {
            return Err(invalid(
                "Pi session publication requires its admitted child",
            ));
        }
        let io = PreparationIo::from_native_work(work)?;
        let (file, retained) = context
            .stage_file_once(
                operation,
                &self.session_path,
                self.session_bytes.as_bytes(),
                "application/x-ndjson",
                "Pi child session",
                Some(&io),
            )
            .await?;
        Ok((file, io.take_native_custody(retained)?))
    }
}

pub(crate) async fn stage_command(
    context: &TaskContext,
    command: OperationId,
    request: &NativeProcessRequest,
    work: &NativeMaterializationWork,
) -> Result<(
    WorkflowCommand,
    TaskEffectPlan,
    acyclic_fs::NativeAllocation,
)> {
    if context.durable_task_id().is_none() {
        return Err(invalid(
            "Pi native launch requires its original durable TaskContext",
        ));
    }
    let maximum = context
        .scope()
        .limits()
        .file_bytes
        .min(crate::MAXIMUM_ADAPTER_STATE_BYTES);
    let mut io = acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
    let (request, custody) = stage_context_artifact(
        context,
        command,
        "request",
        request,
        "application/json",
        maximum,
        work,
    )
    .await?;
    io.absorb_native_custody(custody)?;
    let (schema, custody) = stage_context_artifact(
        context,
        command,
        "schema",
        &result_schema()?,
        "application/schema+json",
        maximum,
        work,
    )
    .await?;
    io.absorb_native_custody(custody)?;
    let plan = TaskEffectPlan {
        provider: io.clone_text(PI_NATIVE_PROVIDER)?,
        guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: io.clone_text(NATIVE_PROCESS_EFFECT_KIND)?,
        request,
        result_schema: schema,
    };
    let (payload, custody) = stage_context_artifact(
        context,
        command,
        "plan",
        &plan,
        "application/json",
        maximum,
        work,
    )
    .await?;
    io.absorb_native_custody(custody)?;
    let command = WorkflowCommand {
        operation_id: command,
        kind: io.clone_text(EFFECT_TASK_COMMAND_KIND)?,
        payload,
    };
    Ok((command, plan, io.into_custody()?))
}

async fn stage_context_artifact(
    context: &TaskContext,
    command: OperationId,
    role: &str,
    value: &impl Serialize,
    media_type: &str,
    maximum: u64,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
    let encoded = crate::pi_root_provider::guarded_pi_bytes(value, maximum, work)?;
    let operation = native_artifact_operation(command, role, work)?;
    let path = work.display(&format_args!("pi-native/{command}/{role}.json"))?;
    let io = PreparationIo::from_native_work(work)?;
    let (file, retained) = context
        .stage_file_once(operation, &path.0, &encoded.0, media_type, role, Some(&io))
        .await?;
    Ok((file, io.take_native_custody(retained)?))
}

fn native_artifact_operation(
    command: OperationId,
    role: &str,
    work: &NativeMaterializationWork,
) -> Result<OperationId> {
    let digest = digest_pi_value(&("acyclic.pi/native-content/v1", command, role), 1024, work)?;
    let mut operation = [0; 16];
    operation.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(OperationId::from_bytes(operation))
}

pub(crate) async fn approve_command(
    protected: &crate::pi_owner::PiOwnerConfig,
    owner: &TaskJournalOwner<acyclic_stream::LocalStream>,
    context: &TaskContext,
    binding: &PiTaskBinding,
    invocation: &PiNativeInvocation,
    manifest: &NativeViewManifest,
    command: OperationId,
    journal: &acyclic_harness::filesystem::FilesystemExecutionJournal<
        acyclic_stream::LocalStream,
        crate::LocalAuthorityBackend,
        crate::LocalObjectBackend,
    >,
    work: &NativeMaterializationWork,
) -> Result<InteractionId> {
    let io = PreparationIo::from_native_work(work)?;
    owner.verify(false, &mut Some(&mut &io)).await?;
    let native_capability = "effect:provider:acyclic.pi.native";
    if context.durable_task_id() != Some(owner.task_id())
        || binding.task_id != owner.task_id()
        || !context.scope().grants().contains(native_capability)
        || !protected.policy().grants.contains(native_capability)
    {
        return Err(Error::Unauthorized(
            "Pi native approval exceeds the original admitted native policy".into(),
        ));
    }
    let runtime = protected.native_runtime()?;
    runtime.validate(work)?;
    if !invocation
        .builtin_tools
        .iter()
        .map(String::as_str)
        .eq(["read", "write", "edit"])
        || super::runtime::require_caller_environment(&invocation.environment).is_err()
    {
        return Err(Error::Unauthorized(
            "Pi native approval changed original protected host settings".into(),
        ));
    }
    let request = invocation.request_with_work(runtime, binding, manifest, work)?;
    let digest = request
        .0
        .approval_digest(owner.task_id(), command, Some(&io))?;
    let approval = task_interaction_id(owner.task_id(), command, &mut Some(&mut &io))?;
    if "Run pinned Pi child ".len() + binding.native_session_id.len() > 256 {
        return Err(acyclic_fs::NativeTreeMaterializationError::InvalidLayout.into());
    }
    let title = work.display(&format_args!(
        "Run pinned Pi child {}",
        binding.native_session_id
    ))?;
    let interaction = Interaction::approval(title.0, command, digest)?;
    journal
        .open_interaction(approval, &interaction, Some(&io))?
        .await?;
    owner.verify(false, &mut Some(&mut &io)).await?;
    let respond = work.display(&format_args!("interaction:respond:{approval}"))?;
    let grants = Capabilities::copy(["interaction:resolve", respond.0.as_str()], work)?;
    let responder = protected.conversation_authority_with_native_work(
        owner.task_id(),
        binding.agent_id,
        &grants.0,
        work,
    )?;
    journal
        .resolve_interaction(
            approval,
            InteractionResponse::Approval {
                approved: true,
                reason: None,
            },
            &responder.0.2,
        )
        .await?;
    owner.verify(false, &mut Some(&mut &io)).await?;
    Ok(approval)
}
