//! Signed operator decisions through the original Root execution.
use super::*;

impl crate::pi_host::PiControlHost<'_> {
    pub(crate) async fn run_operator(
        &self,
        context: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconcile: bool,
        work: &NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        context
            .verify_original_execution_owner_with_native_work(work)?
            .await?;
        let (request, _request_custody) =
            PiOperatorRequest::from_value_with_native_work(&payload, work)?;
        drop(payload);
        validate_approval_request(
            &self.runtime,
            &self.filesystem,
            &self.protected,
            &self.retained.root_session_id,
            &request,
            work,
        )
        .await?;
        let PiOperatorBody::Approval {
            mut prepared,
            digest,
            decision,
        } = request.body
        else {
            return Err(denied("inspect cannot dispatch a Root command"));
        };
        drop(request.operator);
        drop(request.session_id);
        let root = self.protected.policy().root_task;
        let caller = self.caller(&context, &fence, work).await?;
        let owner = &caller.0;
        let admitted = &caller.1;
        if owner.task_id() != root
            || prepared.command.caller != root
            || command.operation_id
                != derived(
                    original_operation(root, root, &prepared.command.sdk_operation, work)?,
                    "selected-writeback-publish",
                    work,
                )?
        {
            return Err(denied(
                "operator control changed its Root, lease or SDK operation",
            ));
        }
        self.protected.validate_root(&admitted.binding, work)?;
        let crate::pi_task::PiTaskExecution::Host {
            root: original_root,
            ..
        } = &admitted.execution
        else {
            return Err(denied("operator Root became a child admission"));
        };
        if admitted.binding.native_session_id != prepared.session
            || admitted.binding.scratch.volume != prepared.private_volume
        {
            return Err(denied(
                "operator approval changed its Root private allocation",
            ));
        }
        let context = crate::pi_root_provider::bind_root_output_context_with_native_work(
            context,
            owner,
            Arc::clone(&self.protected),
            self.resources,
            Arc::clone(&self.filesystem),
            self.stream.clone(),
            &admitted.binding,
            original_root,
            work,
        )
        .await?;
        let io = PreparationIo::from_native_work(work)?;
        let mut history = &io;
        let journal = conversation_journal(
            &self.filesystem,
            &self.stream,
            &self.protected,
            &(&prepared).into(),
            work,
        )?;
        let id = acyclic_harness::durable_host::task_interaction_id(
            root,
            prepared.operation,
            &mut Some(&mut history),
        )?;
        if !reconcile {
            let capability = work.display(&format_args!("interaction:respond:{id}"))?;
            let grants = Capabilities::new_with_native_work(
                ["interaction:resolve", capability.0.as_str()],
                work,
            )?;
            let responder = self.protected.conversation_authority_with_native_work(
                root,
                self.protected.policy().agent,
                &grants.0,
                work,
            )?;
            let response = InteractionResponse::Approval {
                approved: decision == PiOperatorDecision::Approve,
                reason: None,
            };
            owner.verify(false, &mut Some(&mut history)).await?;
            journal
                .resolve_interaction_with_native_work(id, &response, &responder.0.2, work)?
                .await?;
            owner.verify(false, &mut Some(&mut history)).await?;
        }
        if decision == PiOperatorDecision::Decline {
            let outcome = journal.interaction_outcome(id, Some(&io))?.await?;
            if !matches!(outcome, Some(InteractionOutcome::Declined)) {
                return Ok(TaskCommandProgress::Pending);
            }
            let (value, custody) = crate::pi_root_provider::guarded_pi_value(
                &status_response(&prepared, "declined", None),
                crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
                work,
            )?;
            return Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody });
        }
        ToolApprovalVerifier::verify_with_native_work(
            &journal,
            id,
            prepared.operation,
            pi_operator::parse_digest(&prepared.approval_digest)?,
            work,
        )?
        .await?;
        let kind = work.display(&format_args!("acyclic.pi.writeback"))?;
        let publish = WorkflowCommand {
            operation_id: command.operation_id,
            kind: kind.0,
            payload: command.payload,
        };
        prepared.command.approval_digest = Some(digest);
        let (payload, _payload_custody) = crate::pi_root_provider::guarded_pi_value(
            &prepared.command,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            work,
        )?;
        prepared.command.approval_digest = None;
        match self
            .run_selected(context, fence, publish, payload, reconcile, work)
            .await?
        {
            TaskCommandProgress::Ready(value) => drop(value),
            TaskCommandProgress::ReadyWithNativeCustody { value, custody } => {
                drop(value);
                drop(custody);
            }
            TaskCommandProgress::Pending => return Ok(TaskCommandProgress::Pending),
            TaskCommandProgress::Indeterminate => return Ok(TaskCommandProgress::Indeterminate),
        }
        owner.verify(false, &mut Some(&mut history)).await?;
        let (status, native) = observe_writeback(
            &self.filesystem,
            &self.stream,
            self.resources,
            &self.protected,
            &prepared,
            work,
        )
        .await?;
        if status != "published" {
            return Ok(TaskCommandProgress::Pending);
        }
        let (value, custody) = crate::pi_root_provider::guarded_pi_value(
            &status_response(&prepared, status, native.as_ref()),
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            work,
        )?;
        Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody })
    }
}
