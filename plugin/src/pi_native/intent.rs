use super::*;

/// Seeded immutable input consumed only through the actual created child's TaskContext.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeLaunchIntent {
    /// Original caller-selected routing label, not Task/Agent/session authority.
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
        let bytes = context.read_file_with_native_work(reference, work).await?;
        let maximum = context
            .scope()
            .limits()
            .file_bytes
            .min(crate::MAXIMUM_ADAPTER_STATE_BYTES);
        let intent = crate::pi_root_provider::guarded_pi_decode::<Self>(
            bytes.as_ref(),
            maximum,
            std::mem::size_of::<Self>() as u64,
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
        if self.maximum_bytes > crate::MAXIMUM_ADAPTER_STATE_BYTES
            || self.session_path.is_empty()
            || Path::new(&self.session_path).is_absolute()
            || Path::new(&self.session_path)
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

    /// The approved CLI locator and staged SDK bytes must name the same leaf.
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

    /// Publishes on the caller's original work and retains the actual FileRef ownership through the prepared generation's final consumer.
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
        context
            .stage_file_once_with_native_work(
                operation,
                &self.session_path,
                self.session_bytes.as_bytes(),
                "application/x-ndjson",
                "Pi child session",
                work,
            )
            .await
    }
}

/// Child task content is bound through created-resource access, not a root signed scope pre-granted to guessed future child volume names.
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
    // Declared first so every staged payload drops before its combined allocation.
    let mut custody = work.claim(0, 0, acyclic_fs::WorkCounters::default())?;
    let mut request = stage_context_artifact(
        context,
        command,
        "request",
        request,
        "application/json",
        maximum,
        work,
    )
    .await?;
    custody.absorb(&mut request.1)?;
    let mut schema = stage_context_artifact(
        context,
        command,
        "schema",
        &result_schema()?,
        "application/schema+json",
        maximum,
        work,
    )
    .await?;
    custody.absorb(&mut schema.1)?;
    let mut provider = pi_native_string(PI_NATIVE_PROVIDER, work)?;
    custody.absorb(&mut provider.1)?;
    let mut effect_kind = pi_native_string(NATIVE_PROCESS_EFFECT_KIND, work)?;
    custody.absorb(&mut effect_kind.1)?;
    let plan = TaskEffectPlan {
        provider: provider.0,
        guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: effect_kind.0,
        request: request.0,
        result_schema: schema.0,
    };
    let mut payload = stage_context_artifact(
        context,
        command,
        "plan",
        &plan,
        "application/json",
        maximum,
        work,
    )
    .await?;
    custody.absorb(&mut payload.1)?;
    let mut kind = pi_native_string(EFFECT_TASK_COMMAND_KIND, work)?;
    custody.absorb(&mut kind.1)?;
    Ok((
        WorkflowCommand {
            operation_id: command,
            kind: kind.0,
            payload: payload.0,
        },
        plan,
        custody,
    ))
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
    let mut buffer = [0; 96];
    let mut writer = std::io::Cursor::new(buffer.as_mut_slice());
    std::io::Write::write_fmt(&mut writer, format_args!("pi-native/{command}/{role}.json"))
        .map_err(|_| invalid("Pi native artifact locator exceeds its fixed original bound"))?;
    let length = writer.position() as usize;
    let path = std::str::from_utf8(&buffer[..length])
        .map_err(|_| invalid("Pi native artifact locator is not UTF-8"))?;
    let result = context
        .stage_file_once_with_native_work(operation, path, &encoded.0, media_type, role, work)
        .await;
    drop(encoded);
    result
}

fn native_artifact_operation(
    command: OperationId,
    role: &str,
    work: &NativeMaterializationWork,
) -> Result<OperationId> {
    let digest = native_digest(&("acyclic.pi/native-content/v1", command, role), 1024, work)?;
    let mut operation = [0; 16];
    operation.copy_from_slice(&digest[..16]);
    Ok(OperationId::from_bytes(operation))
}

pub(super) fn native_digest(
    value: &impl Serialize,
    maximum: u64,
    work: &NativeMaterializationWork,
) -> Result<[u8; 32]> {
    let encoded = crate::pi_root_provider::canonical_pi_value(value, maximum, work)?;
    work.charge(WorkCounters {
        bytes_hashed: encoded.0.len() as u64,
        ..WorkCounters::default()
    })?;
    let digest = *blake3::hash(&encoded.0).as_bytes();
    drop(encoded);
    Ok(digest)
}

/// The production policy is the original admitted native-effect capability, restricted to the immutable protected host and this exact request.
#[allow(
    clippy::too_many_arguments,
    reason = "original task, immutable request and existing interaction owner"
)]
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
    owner
        .verify(
            false,
            &mut Some(&mut acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )
        .await?;
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
        || require_caller_environment(&invocation.environment).is_err()
    {
        return Err(Error::Unauthorized(
            "Pi native approval changed original protected host settings".into(),
        ));
    }
    let request = invocation.request_with_work(runtime, binding, manifest, work)?;
    let digest = request
        .0
        .approval_digest_with_native_work(owner.task_id(), command, work)?;
    let approval = task_interaction_id(owner.task_id(), command);
    let title = native_fixed_text(format_args!(
        "Run pinned Pi child {}",
        binding.native_session_id
    ))?;
    let title = pi_native_string(
        std::str::from_utf8(&title.0[..title.1]).expect("formatter copies UTF-8"),
        work,
    )?;
    let interaction = (
        Interaction::approval(title.0.into(), command, digest)?,
        title.1,
    );
    journal
        .open_interaction_with_native_work(approval, &interaction.0, work)?
        .await?;
    owner
        .verify(
            false,
            &mut Some(&mut acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )
        .await?;
    let respond = native_fixed_text(format_args!("interaction:respond:{approval}"))?;
    let encoded_grants = crate::pi_root_provider::guarded_pi_bytes(
        &[
            "interaction:resolve",
            std::str::from_utf8(&respond.0[..respond.1]).expect("formatter copies UTF-8"),
        ],
        1024,
        work,
    )?;
    let grants = crate::pi_root_provider::guarded_pi_decode::<Capabilities>(
        &encoded_grants.0,
        1024,
        std::mem::size_of::<Capabilities>() as u64,
        acyclic_harness::wire::ErrorCode::Invalid,
        JsonValueSchema::None,
        NATIVE_CAPABILITIES_DIAGNOSTIC_LABEL_BYTES,
        work,
    )?;
    let responder = protected.conversation_authority_with_native_work(
        owner.task_id(),
        binding.agent_id,
        &grants.0,
        work,
    )?;
    journal
        .resolve_interaction_with_native_work(
            approval,
            &InteractionResponse::Approval {
                approved: true,
                reason: None,
            },
            &responder.0.2,
            work,
        )?
        .await?;
    owner
        .verify(
            false,
            &mut Some(&mut acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )
        .await?;
    Ok(approval)
}

fn native_fixed_text(arguments: std::fmt::Arguments<'_>) -> Result<([u8; 256], usize)> {
    let mut bytes = [0; 256];
    let mut writer = std::io::Cursor::new(bytes.as_mut_slice());
    std::io::Write::write_fmt(&mut writer, arguments).map_err(|_| {
        acyclic_harness::Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout)
    })?;
    let length = writer.position() as usize;
    Ok((bytes, length))
}
