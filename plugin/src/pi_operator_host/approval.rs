//! Prepared writeback tickets on the SDK interaction journal.
use super::*;

fn validate_descriptor(
    protected: &PiOwnerConfig,
    descriptor: &ApprovalRef<'_>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    let root = protected.policy().root_task;
    let original = original_operation(root, root, descriptor.command.sdk_operation, work)?;
    if descriptor.version != 1
        || descriptor.command.caller != root
        || descriptor.command.source == root
        || descriptor.operation != derived(original, "selected-writeback", work)?
        || descriptor.command.approval_digest.is_some()
        || !descriptor.command.confirm
        || descriptor.command.paths.is_empty()
        || descriptor.journal.plan.operation_id
            != acyclic_fs::OperationId::from_bytes(descriptor.operation.into_bytes())
        || descriptor.journal.phase != MaterializationPhase::Prepared
    {
        return Err(denied(
            "operator descriptor is not its Root's prepared writeback",
        ));
    }
    pi_operator::parse_digest(descriptor.approval_digest)?;
    Ok(())
}

pub(super) fn conversation_journal(
    filesystem: &Arc<PiNativeFilesystem>,
    stream: &Arc<LocalStream>,
    protected: &PiOwnerConfig,
    descriptor: &ApprovalRef<'_>,
    work: &NativeMaterializationWork,
) -> Result<(Journal, acyclic_harness::InteractionId)> {
    validate_descriptor(protected, descriptor, work)?;
    let io = PreparationIo::from_native_work(work)?;
    let authority = protected.conversation_authority_with_native_work(
        protected.policy().root_task,
        protected.policy().agent,
        &protected.policy().grants,
        work,
    )?;
    let verifier = authority.0.1.verifier(&mut Some(&mut &io))?;
    let journal = Journal::new(
        stream.clone(),
        Arc::clone(filesystem),
        &SchemaRegistry::new(),
        acyclic_harness::filesystem::FilesystemJournalBinding {
            volume: descriptor.private_volume,
            verifier: &verifier,
            scope: &authority.0.2,
            maximum_bytes: protected.policy().maximum_payload_bytes,
            io: Some(&io),
        },
    )?;
    let id = acyclic_harness::durable_host::task_interaction_id(
        protected.policy().root_task,
        descriptor.operation,
        &mut Some(&mut &io),
    )?;
    Ok((journal, id))
}

pub(crate) fn open_prepared_approval<'a>(
    runtime: &'a PiTaskRuntime,
    filesystem: &'a Arc<PiNativeFilesystem>,
    protected: &'a PiOwnerConfig,
    context_owner: (&'a TaskContext, &'a TaskJournalOwner<LocalStream>),
    binding: &'a PiTaskBinding,
    descriptor: &'a ApprovalRef<'a>,
    work: &'a NativeMaterializationWork,
) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
    let (context, owner) = context_owner;
    async move {
        validate_descriptor(protected, descriptor, work)?;
        runtime.verify_owner_with_native_work(protected, work)?;
        protected.validate_root(binding)?;
        let (task, fence) =
            context.owned_task_binding(Some(&PreparationIo::from_native_work(work)?))?;
        if task != protected.policy().root_task
            || owner.task_id() != task
            || descriptor.session != binding.native_session_id
            || descriptor.private_volume != &binding.scratch.volume
        {
            return Err(denied(
                "prepared approval changed its Root output allocation",
            ));
        }
        let io = PreparationIo::from_native_work(work)?;
        let mut history = &io;
        owner.verify(false, &mut Some(&mut history)).await?;
        let storage = protected.authority(work)?;
        let authority = protected.conversation_authority_with_native_work(
            binding.task_id,
            binding.agent_id,
            context.scope().grants(),
            work,
        )?;
        let verifier = authority.0.1.verifier(&mut Some(&mut history))?;
        let current = runtime
            .runtime
            .task_host()
            .journal_owner(task, fence, Some(&io))
            .await?;
        let journal = Journal::for_task(
            current,
            descriptor.operation,
            context.id(),
            Arc::clone(filesystem),
            acyclic_harness::filesystem::FilesystemJournalBinding {
                volume: &protected.policy().journal,
                verifier: &storage.verifier,
                scope: &storage.signed,
                maximum_bytes: protected.policy().maximum_payload_bytes,
                io: Some(&io),
            },
        )?
        .with_interaction_owner(
            &verifier,
            &SchemaRegistry::new(),
            &authority.0.2,
            descriptor.private_volume,
            Some(&io),
        )?;
        let prompt = work.display(&format_args!(
            "Publish selected Pi writeback {}",
            descriptor.command.sdk_operation,
        ))?;
        let interaction = Interaction::Approval {
            prompt: prompt.0,
            operation_id: descriptor.operation,
            action_digest: pi_operator::parse_digest(descriptor.approval_digest)?,
        };
        let id = acyclic_harness::durable_host::task_interaction_id(
            task,
            descriptor.operation,
            &mut Some(&mut history),
        )?;
        journal
            .open_interaction(id, &interaction, Some(&io))?
            .await?;
        owner.verify(false, &mut Some(&mut history)).await?;
        let _staged = crate::pi_root_provider::stage_pi_input(
            protected,
            filesystem,
            descriptor.operation,
            "operator-approval",
            descriptor,
            work,
        )
        .await?;
        owner.verify(false, &mut Some(&mut history)).await
    }
}

pub(crate) fn require_resolved_approval<'a>(
    filesystem: &'a Arc<PiNativeFilesystem>,
    stream: &'a Arc<LocalStream>,
    protected: &'a PiOwnerConfig,
    descriptor: &'a ApprovalRef<'a>,
    work: &'a NativeMaterializationWork,
) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
    async move {
        let (journal, id) = conversation_journal(filesystem, stream, protected, descriptor, work)?;
        ToolApprovalVerifier::verify(
            &journal,
            id,
            descriptor.operation,
            pi_operator::parse_digest(descriptor.approval_digest)?,
        )
        .await
    }
}

pub(crate) async fn read_prepared_approval(
    runtime: &PiTaskRuntime,
    filesystem: &Arc<PiNativeFilesystem>,
    protected: &PiOwnerConfig,
    sdk_operation: &str,
    work: &NativeMaterializationWork,
) -> Result<Option<(PiPreparedApproval, acyclic_fs::NativeAllocation)>> {
    if sdk_operation.is_empty() || sdk_operation.len() > 512 {
        return Err(denied("status requires its bounded SDK operation"));
    }
    runtime.verify_owner_with_native_work(protected, work)?;
    let root = protected.policy().root_task;
    let operation = derived(
        original_operation(root, root, sdk_operation, work)?,
        "selected-writeback",
        work,
    )?;
    let io = PreparationIo::from_native_work(work)?;
    let context = runtime
        .runtime
        .harness()
        .durable_context(root, OperationId::from_bytes(root.into_bytes()), Some(&io))
        .await?;
    let descriptor: Option<(PiPreparedApproval, _)> = crate::pi_root_provider::read_pi_input(
        protected,
        filesystem,
        operation,
        "operator-approval",
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
        pi_operator::PREPARED_LABEL_BYTES,
        &context,
        work,
    )
    .await?;
    if let Some((descriptor, _)) = &descriptor {
        validate_descriptor(protected, &descriptor.into(), work)?;
    }
    Ok(descriptor)
}
