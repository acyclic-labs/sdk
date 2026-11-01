//! Speculative summaries share the serving owner's original future lifecycle.
use super::*;
use tokio::sync::watch;

/// One selected prefix and the terminal event of its original producer.
pub(crate) struct PendingSummary {
    through: usize,
    source: [u8; 32],
    profile: [u8; 32],
    completion: watch::Receiver<Option<Arc<Result<crate::context::ContextSummary>>>>,
}

impl PendingSummary {
    fn matches(
        &self,
        context: &crate::context::Context,
        profile: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        Ok(self.profile == profile
            && self.through < context.messages.len()
            && context.prefix_digest(self.through, io)? == self.source)
    }

    async fn result(&self, io: Option<&PreparationIo>) -> Result<crate::context::ContextSummary> {
        let mut completion = self.completion.clone();
        let result = completion.wait_for(Option::is_some).await.map_err(|_| {
            let _ = io;
            Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unavailable,
                reason: "background summary producer closed",
            }
        })?;
        match result.as_deref() {
            Some(Ok(summary)) => Ok(summary.clone()),
            Some(Err(Error::Indeterminate(operation))) => Err(Error::Indeterminate(*operation)),
            Some(Err(Error::NativeBoundary { code, reason })) => Err(Error::NativeBoundary {
                code: *code,
                reason: *reason,
            }),
            Some(Err(error)) => Err(Error::NativeBoundary {
                code: error.code(),
                reason: "background summary failed; its original operation retains the cause",
            }),
            None => Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "summary completion has no result",
            )),
        }
    }
}

impl StockExecution<'_> {
    /// Starts at the soft watermark without changing the admitted request.
    pub(super) async fn prefetch_compaction(
        &self,
        journal: &dyn ExecutionJournal,
        context: &crate::context::Context,
        accounting: &ContextAccounting,
        pre_stage: bool,
        policy: &crate::context::ThresholdCompaction,
    ) -> Result<()> {
        if self.stock.session.is_none()
            || self.stock.reconciliation_only
            || context.messages.len() < 3
        {
            return Ok(());
        }
        let canonical = crate::model::PreparedModelRequest::prepare(
            self.request_from_context(context)?,
            self.stock.limits,
            self.io,
        )?;
        let counted;
        let count = if pre_stage {
            counted = stock_count_tokens(self.stock.provider.as_ref(), &canonical, self.io).await?;
            &counted
        } else {
            &accounting.count
        };
        with_history_io(self.io, |work| count.validate(&canonical, work))?;
        let available = accounting
            .capacity
            .context_tokens
            .checked_sub(
                policy.response_reserve_tokens.max(
                    accounting
                        .capacity
                        .output_reservation(self.output_tokens())?,
                ),
            )
            .and_then(|tokens| tokens.checked_sub(count.fixed_tokens))
            .filter(|tokens| *tokens >= 2);
        let Some(available) = available else {
            return Ok(());
        };
        let maximum = context.messages.len();
        let recent_tokens = policy.recent_tokens.min(available / 2);
        let selected = with_history_io(self.io, |work| {
            policy.try_projection_budget(context, count, maximum, recent_tokens, work)
        })?;
        if let Some((through, _)) = selected {
            self.background_summary(journal, context, through)?;
        }
        Ok(())
    }

    /// Reuses only the original selected prefix; changed source cannot consume
    /// a stale result. A live incompatible producer keeps its original custody.
    pub(super) fn background_summary(
        &self,
        journal: &dyn ExecutionJournal,
        context: &crate::context::Context,
        mut through: usize,
    ) -> Result<Option<Arc<PendingSummary>>> {
        let Some(session) = &self.stock.session else {
            return Ok(None);
        };
        if self.stock.reconciliation_only {
            return Ok(None);
        }
        let profile =
            preparation_digest(&(&self.stock.model, self.stock.max_output_tokens), self.io)?;
        let mut selected = session.summary.lock().map_err(|_| {
            stock_boundary(
                self.io,
                crate::wire::ErrorCode::Storage,
                "summary slot is poisoned",
            )
        })?;
        if let Some(original) = selected.as_ref() {
            if original.through == through && original.matches(context, profile, self.io)? {
                let needs_reconciliation = matches!(
                    original.completion.borrow().as_deref(),
                    Some(Err(Error::Indeterminate(_)))
                );
                if !needs_reconciliation {
                    return Ok(Some(Arc::clone(original)));
                }
                // Resume the same accepted summary operation and selected cut.
                // A later consumer is the event that requests reconciliation.
                through = original.through;
            }
            if original.completion.borrow().is_none() && original.completion.has_changed().is_ok() {
                return Ok(None);
            }
        }
        let preparation = session.preparation()?;
        let work = preparation
            .original_native_work()
            .ok_or_else(|| preparation.invalid("summary producer has no serving admission"))?;
        if !self.stock.provider.supports_native_model(work)? {
            return Ok(None);
        }
        let Some(journal) = journal.summary_journal(preparation)? else {
            return Ok(None);
        };
        let source = context.prefix_digest(through, self.io)?;
        let mut source_preparation = StockExecution::ordinary(self.stock);
        source_preparation.io = Some(preparation);
        let material = source_preparation.prepare_summary_source(context, through)?;
        let mut identity = blake3::Hasher::new_derive_key("acyclic.harness.background-summary.v1");
        identity.update(&session.identity);
        identity.update(&profile);
        identity.update(&source);
        let mut operation = [0; 16];
        operation.copy_from_slice(&identity.finalize().as_bytes()[..16]);
        let operation = OperationId::from_bytes(operation);
        // A summary owns no foreground task context, stop token, tool effects,
        // reply writer or hook invocation. Content uses the same journal grant.
        let mut stock = StockExecutor::new(
            self.stock.model.clone(),
            Arc::clone(&self.stock.provider),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_limits(self.stock.limits)
        .with_compaction_policy(self.compaction_policy()?.clone());
        stock.max_output_tokens = self.stock.max_output_tokens;
        let (publish, completion) = watch::channel(None);
        let pending = Arc::new(PendingSummary {
            through,
            source,
            profile,
            completion,
        });
        session.attach(move |slot| async move {
            let result = async {
                let io = slot.preparation()?;
                let ownership = journal.claim_execution(operation, Some(io))?;
                let mut execution = StockExecution::ordinary(&stock);
                execution.io = Some(io);
                let mut phase = DispatchPhase::new();
                let (summary, text) = execution
                    .summarize(journal.as_ref(), None, operation, 0, material, &mut phase)
                    .await?;
                execution.retire_step_text(text)?;
                ownership.release();
                Ok(summary)
            }
            .await;
            publish.send_replace(Some(Arc::new(result)));
        })?;
        let previous = selected.replace(Arc::clone(&pending));
        drop(selected);
        drop(previous);
        Ok(Some(pending))
    }

    pub(super) async fn demand_summary(
        &self,
        pending: &PendingSummary,
        context: &crate::context::Context,
    ) -> Result<(crate::context::ContextSummary, usize)> {
        let summary = pending.result(self.io).await?;
        if summary.source_messages as usize != pending.through
            || summary.source_digest != pending.source
        {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Storage,
                "background summary differs from its selected source",
            ));
        }
        let mandatory = with_history_io(self.io, |work| {
            crate::context::mandatory_message_count(context, pending.through, work)
        })?;
        let maximum = mandatory.checked_add(1).ok_or_else(|| {
            stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "summary projection count overflows",
            )
        })?;
        // The existing compaction validator reconstructs this exact projection,
        // including complete tool exchanges and mandatory instructions.
        Ok((summary, maximum))
    }
}
