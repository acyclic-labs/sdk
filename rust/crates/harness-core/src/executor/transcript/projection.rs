//! The single durable projection codec retains main's inline context schema.
use super::*;

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32"), not(loom)))]
mod tests;

impl ResponseProjection {
    pub(in crate::executor) async fn continuation_metadata(
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
        original: Option<&PreparationIo>,
    ) -> Result<Self> {
        #[cfg(feature = "filesystem")]
        let scope = crate::filesystem::PreparationScope::new(original)?;
        #[cfg(feature = "filesystem")]
        let io = scope.io();
        #[cfg(not(feature = "filesystem"))]
        let io = original;
        let stored = retain_preparation(load_json::<Self>(journal, reference, io).await?)?;
        let projection = Self {
            context: crate::context::Context {
                messages: Vec::new(),
                metadata: stored.context.metadata,
                current_input_index: stored.context.current_input_index,
            },
            canonical: None,
            checkpoint: None,
            working: stored.working,
            accounting: None,
        };
        drop((
            stored.context.messages,
            stored.canonical,
            stored.checkpoint,
            stored.accounting,
        ));
        #[cfg(feature = "filesystem")]
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
    #[cfg(feature = "filesystem")]
    let scope = crate::filesystem::PreparationScope::new(original)?;
    #[cfg(feature = "filesystem")]
    let io = scope.io();
    #[cfg(not(feature = "filesystem"))]
    let io = original;
    let projection =
        retain_preparation(load_json::<ResponseProjection>(journal, reference, io).await?)?;
    #[cfg(feature = "filesystem")]
    let retained = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
    crate::context::validate_projected_context(&projection.context, limits, io)?;
    #[cfg(feature = "filesystem")]
    let hold = {
        // The decoder already inventoried this graph. Transfer its original
        // storage; only validation scratch belongs to the temporary scope.
        scope.finish(retained)?;
        original.map(|io| io.guard_owned(retained)).transpose()?
    };
    #[cfg(not(feature = "filesystem"))]
    let hold = None;
    Ok((projection, hold))
}

impl StockExecution<'_> {
    pub(in crate::executor) async fn load_response_projection(
        &self,
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
    ) -> Result<(ResponseProjection, crate::model::PreparedModelRequest)> {
        let mut projection = retain_preparation(
            load_projection(journal, reference, self.stock.limits, self.io).await?,
        )?;
        let request =
            self.request_from_messages(std::mem::take(&mut projection.context.messages))?;
        let prepared =
            crate::model::PreparedModelRequest::prepare(request, self.stock.limits, self.io)?;
        Ok((projection, prepared))
    }
}
