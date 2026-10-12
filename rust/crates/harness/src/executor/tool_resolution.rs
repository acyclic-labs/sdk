use super::*;

impl StockExecution<'_> {
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
            crate::contract::preparation_format(
                format_args!("tool:{step}:{call_id}:failed:{}", OperationId::new()),
                work,
            )
        })?;
        let event = ExecutionEvent::ToolFailed {
            step,
            call_id: stock_string_copy(call_id, self.io)?,
            reason,
        };
        match stock_append_if_tail(journal, operation_id, tail, &key, &event, self.io).await {
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
    ) -> Result<ToolCallProgress> {
        let span = obs_span!("acyclic.harness.executor.tool_call", step = step);
        let call = self.settle_tool_call(journal, operation_id, step, invocation, prior_messages);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native.is_some() {
            return span.scope(call).await;
        }
        traced(span, call).await
    }
}
