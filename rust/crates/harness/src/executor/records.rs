use super::*;

impl StockExecution<'_> {
    pub(super) async fn prefix_records(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
    ) -> Result<Vec<ExecutionRecord>> {
        let maximum = self
            .stock
            .limits
            .model_events_per_step
            .saturating_add(self.stock.limits.tool_calls_per_step.saturating_mul(3))
            .saturating_add(4);
        let (_, records) = replay_execution(
            journal,
            operation_id,
            maximum,
            |event| match event {
                ExecutionEvent::Started { .. } => true,
                ExecutionEvent::ModelStarted {
                    step: recorded,
                    purpose: ModelPurpose::Response,
                    ..
                }
                | ExecutionEvent::Model {
                    step: recorded,
                    purpose: ModelPurpose::Response,
                    ..
                }
                | ExecutionEvent::ToolStarted { step: recorded, .. }
                | ExecutionEvent::ToolCompleted { step: recorded, .. }
                | ExecutionEvent::ToolFailed { step: recorded, .. }
                | ExecutionEvent::ContextPrepared { step: recorded, .. }
                | ExecutionEvent::ContextCompacted { step: recorded, .. } => *recorded == step,
                ExecutionEvent::ModelStarted { .. } | ExecutionEvent::Model { .. } => false,
            },
            self.io,
        )
        .await?;
        Ok(records)
    }

    pub(super) async fn model_records(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        purpose: ModelPurpose,
    ) -> Result<(u64, Vec<ExecutionRecord>)> {
        replay_execution(
            journal,
            operation_id,
            self.stock.limits.model_events_per_step.saturating_add(3),
            |event| {
                matches!(event,
                ExecutionEvent::ModelStarted { step: event_step, purpose: event_purpose, .. }
                | ExecutionEvent::Model { step: event_step, purpose: event_purpose, .. }
                if *event_step == step && *event_purpose == purpose)
                    || matches!(event, ExecutionEvent::ContextPrepared { step: event_step, .. }
                    | ExecutionEvent::ContextCompacted { step: event_step, .. }
                    if *event_step == step && purpose == ModelPurpose::Response)
            },
            self.io,
        )
        .await
    }

    pub(super) async fn tool_records(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        call_id: &str,
    ) -> Result<(u64, Vec<ExecutionRecord>)> {
        replay_execution(
            journal,
            operation_id,
            3,
            |event| {
                matches!(event,
            ExecutionEvent::ToolStarted { step: event_step, call_id: existing, .. }
            | ExecutionEvent::ToolCompleted { step: event_step, call_id: existing, .. }
            | ExecutionEvent::ToolFailed { step: event_step, call_id: existing, .. }
            if *event_step == step && existing == call_id)
            },
            self.io,
        )
        .await
    }
}
