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
        #[cfg(feature = "filesystem")]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let context = crate::context::copy_context(
            &crate::context::Context {
                messages: Vec::new(),
                metadata: stored.context.metadata,
                current_input_index: stored.context.current_input_index,
            },
            io,
        )?;
        let working = stored
            .working
            .as_ref()
            .map(|file| crate::model::clone_file(file, io))
            .transpose()?;
        drop((
            stored.context.messages,
            stored.canonical,
            stored.checkpoint,
            stored.working,
            stored.accounting,
        ));
        #[cfg(feature = "filesystem")]
        {
            let retained = io.map_or(Ok(0), |io| {
                io.owned_bytes()?
                    .checked_sub(before)
                    .ok_or_else(|| invalid(Some(io)))
            })?;
            scope.finish(retained)?;
        }
        let projection = Self {
            context,
            canonical: None,
            checkpoint: None,
            working,
            accounting: None,
        };
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
    let stored =
        retain_preparation(load_json::<ResponseProjection>(journal, reference, io).await?)?;
    crate::context::validate_projected_context(&stored.context, limits, io)?;
    #[cfg(feature = "filesystem")]
    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
    let projection = ResponseProjection {
        context: crate::context::copy_context(&stored.context, io)?,
        canonical: stored
            .canonical
            .as_ref()
            .map(|file| crate::model::clone_file(file, io))
            .transpose()?,
        checkpoint: stored
            .checkpoint
            .as_ref()
            .map(|file| crate::model::clone_file(file, io))
            .transpose()?,
        working: stored
            .working
            .as_ref()
            .map(|file| crate::model::clone_file(file, io))
            .transpose()?,
        accounting: stored
            .accounting
            .as_ref()
            .map(|accounting| accounting.copy(io))
            .transpose()?,
    };
    drop(stored);
    #[cfg(feature = "filesystem")]
    let hold = {
        let retained = io.map_or(Ok(0), |io| {
            io.owned_bytes()?
                .checked_sub(before)
                .ok_or_else(|| invalid(Some(io)))
        })?;
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
