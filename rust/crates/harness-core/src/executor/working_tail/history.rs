use super::*;

impl StockExecution<'_> {
    pub(in crate::executor) async fn begin_working_history(&self) {
        let mut cache = self.working_tail.lock().await;
        cache.retained = None;
        cache.prior_offset = 0;
    }

    pub(in crate::executor) async fn absolute_prior_count(&self, messages: usize) -> Result<u64> {
        let offset = self.working_tail.lock().await.prior_offset;
        self.prior_count(offset, messages)
    }

    pub(super) fn prior_count(&self, offset: u64, messages: usize) -> Result<u64> {
        u64::try_from(messages)
            .ok()
            .and_then(|count| offset.checked_add(count))
            .ok_or_else(|| self.invalid_working_cut())
    }

    /// Tool invocation indexes are relative to the current tail; callers
    /// retire only after every invocation in that step has settled.
    pub(in crate::executor) async fn retire_working_history(
        &self,
        messages: &mut Vec<ModelMessage>,
    ) -> Result<()> {
        let mut cache = self.working_tail.lock().await;
        if let Some(retained) = &cache.retained {
            let through = retained.cut.prior_messages;
            retirement::retire_prefix(messages, &mut cache.prior_offset, through, self.io)?;
        }
        Ok(())
    }
}
