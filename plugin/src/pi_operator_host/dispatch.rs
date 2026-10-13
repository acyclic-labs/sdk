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
        context.verify_original_execution_owner(work).await?;
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
        if context.durable_task_id() != Some(root)
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
        let actual = self.context(context, &fence, work).await?;
        let (context, owner, admitted, _) = &actual;
        self.protected.validate_root(&admitted.binding, work)?;
        if admitted.binding.native_session_id != prepared.session
            || admitted.binding.scratch.volume != prepared.private_volume
        {
            return Err(denied(
                "operator approval changed its Root private allocation",
            ));
        }
        let io = PreparationIo::from_native_work(work)?;
        let mut history = &io;
        let (journal, id) = conversation_journal(
            &self.filesystem,
            &self.stream,
            &self.protected,
            &(&prepared).into(),
            work,
        )?;
        if !reconcile {
            let capability = work.display(&format_args!("interaction:respond:{id}"))?;
            let grants = Capabilities::copy(["interaction:resolve", capability.0.as_str()], work)?;
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
                .resolve_interaction(id, response, &responder.0.2)
                .await?;
            owner.verify(false, &mut Some(&mut history)).await?;
        }
        if decision == PiOperatorDecision::Decline {
            let outcome = journal.interaction_outcome(id, Some(&io))?.await?;
            if !matches!(outcome, Some(InteractionOutcome::Declined)) {
                return Ok(TaskCommandProgress::Pending);
            }
            return ready(&status_response(&prepared, "declined", None), work);
        }
        ToolApprovalVerifier::verify(
            &journal,
            id,
            prepared.operation,
            pi_operator::parse_digest(&prepared.approval_digest)?,
        )
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
            .run_selected(context.clone(), fence, publish, payload, reconcile, work)
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
        ready(&status_response(&prepared, status, native.as_ref()), work)
    }
}
