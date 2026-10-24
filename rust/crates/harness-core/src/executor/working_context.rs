//! Continue an operation from its published canonical cut, before context stages.
use super::*;

pub(super) struct WorkingContext<'a> {
    journal: Option<ExecutionJournalCacheId>,
    operation: OperationId,
    selection: crate::conversation::ModelContextSelection,
    context: crate::context::Context,
    // Values are dropped before the custody funding their retained storage.
    _hold: Option<crate::conversation::ContentHold<'a>>,
}

impl<'a> StockExecution<'a> {
    pub(super) async fn canonical_base_for_turn(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        input: &mut ContextInput,
    ) -> Result<crate::context::Context> {
        if input.step == 0 || input.selected_context.is_none() {
            return self.canonical_base_for_input(journal, input).await;
        }
        if let Some(selected) = &input.selected_context {
            selected.validate_for_input(&input.input, self.io)?;
        }
        let uncached;
        let working = if journal.cache_identity().is_some() {
            self.working_context
                .get_or_try_init(|| self.load_working_context(journal, operation, input))
                .await?
        } else {
            uncached = self.load_working_context(journal, operation, input).await?;
            &uncached
        };
        let Some(working) = working else {
            return self.canonical_base_for_input(journal, input).await;
        };
        if working.journal != journal.cache_identity()
            || working.operation != operation
            || input
                .selected_context
                .as_ref()
                .is_none_or(|selected| selected.selection != working.selection)
        {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "working checkpoint differs from the admitted selection",
            ));
        }
        let context = with_history_io(self.io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let (messages, claim) = crate::context::clone_messages(
                &working.context.messages,
                None,
                &input.prior_messages,
                quota.work(),
            )?;
            quota.adopt(claim)?;
            let (metadata, claim) =
                crate::context::clone_metadata(&working.context.metadata, quota.work())?;
            quota.adopt(claim)?;
            quota.transfer();
            Ok::<_, Error>(crate::context::Context {
                messages,
                metadata,
                current_input_index: working.context.current_input_index,
            })
        })?;
        crate::context::validate_projected_context(&context, self.stock.limits, self.io)?;
        Ok(context)
    }

    async fn load_working_context(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        input: &ContextInput,
    ) -> Result<Option<WorkingContext<'a>>> {
        #[cfg(feature = "filesystem")]
        let scope = crate::filesystem::PreparationScope::new(self.io)?;
        #[cfg(feature = "filesystem")]
        let io = scope.io();
        #[cfg(not(feature = "filesystem"))]
        let io = self.io;
        let Some(reference) =
            canonical_checkpoint_for_operation(journal, operation, self.stock.limits, io).await?
        else {
            return Ok(None);
        };
        let (envelope, retained) = load_canonical_checkpoint(
            journal,
            &reference,
            self.stock.limits,
            self.io.map(|_| &self.stock.tool_scope),
            io,
        )
        .await?;
        if envelope.operation_id != operation
            || input
                .selected_context
                .as_ref()
                .is_none_or(|selected| selected.selection != envelope.selection)
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "working checkpoint differs from the admitted selection",
            ));
        }
        #[cfg(feature = "filesystem")]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let selection = stock_selection_copy(&envelope.selection, io)?;
        let context = crate::context::copy_context(&retained, io)?;
        drop(envelope);
        drop(retained);
        drop(reference);
        #[cfg(feature = "filesystem")]
        let hold = {
            let retained = io.map_or(Ok(0), |io| {
                io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Storage,
                        "working context ownership underflows",
                    )
                })
            })?;
            scope.finish(retained)?;
            self.io.map(|io| io.guard_owned(retained)).transpose()?
        };
        #[cfg(not(feature = "filesystem"))]
        let hold = None;
        Ok(Some(WorkingContext {
            journal: journal.cache_identity(),
            operation,
            selection,
            context,
            _hold: hold,
        }))
    }
}
