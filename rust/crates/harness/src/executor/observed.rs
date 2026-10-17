use super::*;

impl StockExecution<'_> {
    /// Derives, from this turn's canonical journal alone, the latest version of
    /// each path that a completed tool result showed the model. Nothing is
    /// cached: execution, a resumed turn and reconciliation all derive the same
    /// set from the same records, and failed calls never contribute because
    /// only `ToolCompleted` results are read.
    pub(super) async fn observed_files(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
    ) -> Result<Vec<FileRef>> {
        let maximum = self
            .stock
            .limits
            .model_steps
            .saturating_mul(self.stock.limits.tool_calls_per_step)
            .saturating_mul(2);
        let (_, records, _hold) = replay_execution(
            journal,
            operation_id,
            maximum,
            |event| {
                matches!(
                    event,
                    ExecutionEvent::ToolStarted { .. } | ExecutionEvent::ToolCompleted { .. }
                )
            },
            self.io,
        )
        .await?;
        let mut observed: Vec<FileRef> = Vec::new();
        for record in &records {
            let ExecutionEvent::ToolCompleted {
                step,
                call_id,
                result,
                ..
            } = &record.event
            else {
                continue;
            };
            let invocation = records
                .iter()
                .find_map(|started| match &started.event {
                    ExecutionEvent::ToolStarted {
                        step: recorded,
                        call_id: started_call,
                        invocation,
                    } if recorded == step && started_call == call_id => Some(invocation),
                    _ => None,
                })
                .ok_or_else(|| {
                    stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "completed tool has no admitted invocation",
                    )
                })?;
            let (invocation, _) =
                load_json::<crate::tool::RecordedInvocation>(journal, invocation, self.io).await?;
            let Ok(tool) =
                stock_tool_lookup(&self.stock.tools, &invocation.invocation.name, self.io)
            else {
                continue;
            };
            let (result, _) = load_json::<ToolResult>(journal, result, self.io).await?;
            if let Some(file) = tool.executor.observed_file(&result) {
                observed
                    .retain(|seen| seen.volume() != file.volume() || seen.path() != file.path());
                stock_vec_push(&mut observed, file, self.io)?;
            }
        }
        Ok(observed)
    }
}
