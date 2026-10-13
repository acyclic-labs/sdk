use super::*;

impl StockExecution<'_> {
    pub(super) async fn settle_approval(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        approval_operation: OperationId,
        request: &Interaction,
        invocation: &ToolInvocation,
        started: bool,
    ) -> Result<Option<ExecutionProgress<()>>> {
        if !self
            .stock
            .tool_scope
            .grants()
            .contains(capability::INTERACTION_ROUTE)
        {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Unauthorized,
                "stock tool approval requires interaction:route",
            ));
        }
        let task = self.stock.task.as_ref().map_or(
            TaskId::from_bytes(operation.into_bytes()),
            |(_, task, _)| *task,
        );
        let approval = with_history_io(self.io, |work| {
            crate::durable_host::task_interaction_id(task, approval_operation, work)
        })?;
        journal
            .open_interaction(approval, request, self.io)?
            .await?;
        let Some(outcome) = journal.interaction_outcome(approval, self.io)?.await? else {
            return if started {
                Err(Error::Indeterminate(invocation.operation_id))
            } else {
                Ok(Some(ExecutionProgress::Pending(invocation.operation_id)))
            };
        };
        if stock_is_native(self.io) && matches!(outcome, InteractionOutcome::Answered { .. }) {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "approval resolved with a question answer",
            ));
        }
        match check_tool_approval(outcome) {
            Ok(()) => Ok(None),
            Err(Error::InteractionRejected(reason)) if !started => {
                Ok(Some(ExecutionProgress::Rejected(reason)))
            }
            Err(Error::InteractionRejected(_)) => {
                Err(Error::Indeterminate(invocation.operation_id))
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn record_tool_failure(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        call_id: &str,
        reason: ToolFailureKind,
    ) -> Result<()> {
        let (tail, current) = self
            .tool_records(journal, operation_id, step, call_id)
            .await?;
        if current.iter().any(|record| {
            matches!(&record.event,
            ExecutionEvent::ToolCompleted { step: event_step, call_id: existing, .. }
                | ExecutionEvent::ToolFailed { step: event_step, call_id: existing, .. }
                if *event_step == step && existing == call_id)
        }) {
            return Err(Error::Indeterminate(operation_id));
        }
        let key = with_history_io(self.io, |work| {
            crate::executor::preparation_format(
                format_args!("tool:{step}:{call_id}:failed:{}", OperationId::new()),
                work,
            )
        })?;
        let event = ExecutionEvent::ToolFailed {
            step,
            call_id: stock_string_copy(call_id, self.io)?,
            reason,
        };
        match journal
            .append_if_tail(operation_id, tail, &key, &event, self.io)?
            .await
        {
            Ok(true) => Ok(()),
            Ok(false) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                Err(Error::Indeterminate(operation_id))
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn resolve_tool_call(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        invocation: ToolInvocation,
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<ExecutionProgress<()>> {
        let span = obs_span!("acyclic.harness.executor.tool_call", step = step);
        let call = self.settle_tool_call(journal, operation_id, step, invocation, prior_messages);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_work().is_some() {
            return span.scope(call).await;
        }
        traced(span, call).await
    }
}
