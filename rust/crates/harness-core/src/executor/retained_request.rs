//! A stock turn owns its last admitted request until the following step.
//! Public independent model steps and replaceable context stages never infer
//! that an arbitrary caller supplied the same history twice.
use super::*;

#[derive(Default)]
pub(super) struct RequestCache<'a> {
    enabled: bool,
    retained: Option<RetainedRequest<'a>>,
}

struct RetainedRequest<'a> {
    operation: OperationId,
    journal: ExecutionJournalCacheId,
    step: u32,
    prior: u64,
    request: crate::model::PreparedModelRequest,
    // Request storage dies before its original allocation custody.
    hold: Option<crate::conversation::ContentHold<'a>>,
}

impl<'a> StockExecution<'a> {
    pub(super) async fn begin_request_continuation(&self) {
        let mut cache = self.request_cache.lock().await;
        cache.retained = None;
        cache.enabled = self.context_pipeline().is_empty();
    }

    pub(super) async fn retain_response_request(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior: usize,
        request: crate::model::PreparedModelRequest,
    ) -> Result<()> {
        let Some(identity) = journal.cache_identity() else {
            return Ok(());
        };
        let mut cache = self.request_cache.lock().await;
        if !cache.enabled {
            return Ok(());
        }
        let prior = self.absolute_prior_count(prior).await?;
        #[cfg(feature = "filesystem")]
        let hold = self
            .io
            .map(|io| io.guard_owned(request.preparation_owned_bytes(io)?))
            .transpose()?;
        #[cfg(not(feature = "filesystem"))]
        let hold = None;
        cache.retained = Some(RetainedRequest {
            operation: input.operation_id,
            journal: identity,
            step,
            prior,
            request,
            hold,
        });
        Ok(())
    }

    pub(super) async fn continue_response_request(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior: &[ModelMessage],
    ) -> Result<Option<(ResponseProjection, crate::model::PreparedModelRequest)>> {
        let mut cache = self.request_cache.lock().await;
        let Some(retained) = cache.retained.take() else {
            return Ok(None);
        };
        if !cache.enabled
            || Some(retained.journal) != journal.cache_identity()
            || retained.operation != input.operation_id
            || retained.step.checked_add(1) != Some(step)
        {
            return Ok(None);
        }
        let total = self.absolute_prior_count(prior.len()).await?;
        let suffix_count = total
            .checked_sub(retained.prior)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| self.invalid_working_cut())?;
        let suffix = prior
            .len()
            .checked_sub(suffix_count)
            .and_then(|start| prior.get(start..))
            .ok_or_else(|| self.invalid_working_cut())?;
        let previous = self
            .previous_request_projection(journal, input.operation_id, retained.step)
            .await?;
        let projection =
            ResponseProjection::continuation_metadata(journal, &previous, self.io).await?;
        self.synchronize_working_cut(
            journal,
            input,
            step,
            prior.len(),
            projection.working.as_ref(),
        )
        .await?;
        if projection
            .context
            .current_input_index
            .is_none_or(|index| index as usize >= retained.request.request().messages.len())
        {
            return Err(self.invalid_working_cut());
        }
        let RetainedRequest { request, hold, .. } = retained;
        #[cfg(feature = "filesystem")]
        if let Some(hold) = hold {
            hold.into_retained()?;
        }
        #[cfg(not(feature = "filesystem"))]
        let _ = hold;
        let mut continued = request.into_continuation();
        *continued.messages() = crate::model::clone_messages(suffix, self.io)?;
        Ok(Some((
            projection,
            continued.finish(self.stock.limits, self.io)?,
        )))
    }

    async fn previous_request_projection(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
    ) -> Result<FileRef> {
        let (_, records) = self
            .model_records(journal, operation, step, ModelPurpose::Response)
            .await?;
        let mut prepared = None;
        let mut compacted = None;
        for record in &records {
            let previous = match &record.event {
                ExecutionEvent::ContextPrepared { projection, .. } => prepared.replace(projection),
                ExecutionEvent::ContextCompacted { projection, .. } => {
                    compacted.replace(projection)
                }
                _ => continue,
            };
            if previous.is_some() {
                return Err(self.invalid_working_cut());
            }
        }
        if prepared.is_none() {
            return Err(self.invalid_working_cut());
        }
        crate::model::clone_file(
            compacted
                .or(prepared)
                .ok_or_else(|| self.invalid_working_cut())?,
            self.io,
        )
    }
}
