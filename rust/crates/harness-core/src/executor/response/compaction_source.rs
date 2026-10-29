use super::*;

impl StockExecution<'_> {
    /// A private working cut contains the pre-stage base. If only stage output
    /// creates pressure, compact the projected context under the existing
    /// stage contract instead of treating stage output as retained history.
    pub(super) async fn response_compaction_source(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32, &[ModelMessage]),
        projection: &ResponseProjection,
        accounting: &ContextAccounting,
        policy: &crate::context::ThresholdCompaction,
    ) -> Result<(crate::context::Context, bool)> {
        let selected_base = turn.1 == 0 && turn.0.selected_context.is_some();
        if selected_base
            || projection.working.is_some()
            || journal.working_checkpoint_authority().is_some()
        {
            let (input, step, prior) = turn;
            let (_, base, _) = self.response_base(journal, input, step, prior).await?;
            if selected_base {
                return Ok((base, true));
            }
            let request = crate::model::PreparedModelRequest::prepare(
                self.request_from_context(&base)?,
                self.stock.limits,
                self.io,
            )?;
            let count = stock_count_tokens(self.stock.provider.as_ref(), &request, self.io).await?;
            let tokens = with_history_io(self.io, |work| count.validate(&request, work))?;
            if policy.needs_compaction(accounting.capacity, tokens)? {
                return Ok((base, true));
            }
        }
        Ok((
            crate::context::copy_context(&projection.context, self.io)?,
            false,
        ))
    }
}
