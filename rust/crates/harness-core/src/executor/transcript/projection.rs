//! Versioned projections retain the original admitted request header and context.
use super::*;

#[cfg(all(test, not(target_arch = "wasm32"), not(loom)))]
mod tests;

impl ResponseProjection {
    /// Recover the admitted steering without imposing a new context policy.
    pub(crate) async fn steering_snapshot(
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
    ) -> Result<Option<crate::agent::SteeringSnapshot>> {
        Ok(load_projection_record(journal, reference, None)
            .await?
            .projection
            .steering)
    }

    pub(in crate::executor) async fn continuation_metadata(
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
        original: Option<&PreparationIo>,
    ) -> Result<Self> {
        let scope = crate::filesystem::PreparationScope::new(original)?;
        let io = scope.io();
        let stored = load_projection_record(journal, reference, io)
            .await?
            .projection;
        let projection = Self {
            context: crate::context::Context {
                messages: Vec::new(),
                metadata: stored.context.metadata,
                current_input_index: stored.context.current_input_index,
            },
            steering: None,
            canonical: None,
            checkpoint: None,
            working: stored.working,
            accounting: None,
        };
        drop((
            stored.context.messages,
            stored.steering,
            stored.canonical,
            stored.checkpoint,
            stored.accounting,
        ));
        {
            let retained = projection
                .decoded_owned_bytes()?
                .ok_or_else(|| invalid(io))?;
            scope.finish(retained)?;
        }
        Ok(projection)
    }
}

pub(crate) async fn load_projection<'a>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    original: Option<&'a PreparationIo>,
) -> Result<(
    ResponseProjection,
    Option<crate::conversation::ContentHold<'a>>,
)> {
    let scope = crate::filesystem::PreparationScope::new(original)?;
    let io = scope.io();
    let projection = load_projection_record(journal, reference, io)
        .await?
        .projection;

    let retained = projection
        .decoded_owned_bytes()?
        .ok_or_else(|| invalid(io))?;
    crate::context::validate_projected_context(&projection.context, limits, io)?;
    let hold = {
        // The header is no longer retained by checkpoint readers.
        scope.finish(retained)?;
        original.map(|io| io.guard_owned(retained)).transpose()?
    };
    Ok((projection, hold))
}

impl StockExecution<'_> {
    pub(in crate::executor) async fn load_response_projection(
        &self,
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
    ) -> Result<(ResponseProjection, crate::model::PreparedModelRequest)> {
        let scope = crate::filesystem::PreparationScope::new(self.io)?;

        let io = scope.io();
        let ProjectionRecord {
            header,
            mut projection,
            ..
        } = load_projection_record(journal, reference, io).await?;
        crate::context::validate_projected_context(&projection.context, self.stock.limits, io)?;
        let request = ModelRequest {
            summary: header.summary,
            compose_tools: header.compose_tools,
            model: header.model,
            tools: header.tools,
            sampling: header.sampling,
            max_output_tokens: header.max_output_tokens,
            messages: std::mem::take(&mut projection.context.messages),
        };
        let prepared = crate::model::PreparedModelRequest::prepare(request, self.stock.limits, io)?;

        {
            let retained = crate::core::owned_sum([
                projection
                    .decoded_owned_bytes()?
                    .ok_or_else(|| invalid(io))?,
                io.map_or(Ok(0), |io| prepared.preparation_owned_bytes(io))?,
            ])?;
            scope.finish(retained)?;
        }
        Ok((projection, prepared))
    }
}
