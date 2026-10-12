use super::*;

/// Owner-bound durable session for one exact tool invocation.
pub struct ResumableToolSession {
    pub(super) definition: ToolDefinition,
    pub(super) invocation: ToolInvocation,
    pub(super) host: DurableWorkflowHost,
}

impl ResumableToolSession {
    /// Returns the immutable admitted invocation.
    #[must_use]
    pub const fn invocation(&self) -> &ToolInvocation {
        &self.invocation
    }

    /// Returns the latest committed checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &MachineCheckpoint {
        self.host.checkpoint()
    }

    /// Commits one state transition and its complete ref-only command outbox.
    pub async fn step(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        input: Value,
    ) -> Result<MachineTransition> {
        let output_schema = &self.definition.output_schema;
        self.host
            .step(operation_id, idempotency_key, input, |transition| {
                if let MachineStatus::Completed { value } = &transition.status {
                    crate::executor::validate_json_schema_value(output_schema, value, "tool output", &mut None)?;
                }
                Ok(())
            })
            .await
    }
}
