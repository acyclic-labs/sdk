use super::*;

pub(in crate::executor) struct StockExecution<'a> {
    pub(in crate::executor) stock: &'a StockExecutor,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::executor) native:
        Option<(&'a Arc<crate::runtime::TaskContext>, OperationId)>,
    pub(in crate::executor) io: Option<&'a PreparationIo>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::executor) interactions: Option<&'a Arc<dyn crate::runtime::InteractionRouter>>,
    pub(in crate::executor) summary: Option<(ContextPipeline, Option<u32>)>,
    pub(in crate::executor) reconciliation_only: bool,
}

impl<'a> StockExecution<'a> {
    pub(in crate::executor) fn ordinary(stock: &'a StockExecutor) -> Self {
        Self {
            stock,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            interactions: None,
            io: None,
            summary: None,
            reconciliation_only: stock.reconciliation_only,
        }
    }
    pub(in crate::executor) fn io(&self) -> Option<&PreparationIo> {
        self.io
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::executor) fn native_work(
        &self,
    ) -> Option<&acyclic_fs::NativeMaterializationWork> {
        self.io().and_then(PreparationIo::original_native_work)
    }
    pub(in crate::executor) fn original_context(&self) -> Option<&crate::runtime::TaskContext> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some((native, _)) = self.native {
            return Some(native.as_ref());
        }
        self.stock.task_context.as_ref().map(|(context, _)| context)
    }
    pub(in crate::executor) fn context_pipeline(&self) -> &ContextPipeline {
        self.summary
            .as_ref()
            .map_or(&self.stock.context, |(pipeline, _)| pipeline)
    }
    pub(in crate::executor) fn compaction_policy(&self) -> &crate::context::CompactionPolicy {
        if self.summary.is_some() {
            &crate::context::CompactionPolicy::Disabled
        } else {
            &self.stock.compaction
        }
    }
    pub(in crate::executor) fn output_tokens(&self) -> Option<u32> {
        self.summary
            .as_ref()
            .map_or(self.stock.max_output_tokens, |(_, tokens)| *tokens)
    }
    pub(in crate::executor) fn inherited_prefix(
        &self,
    ) -> Option<&(
        FileRef,
        Arc<dyn crate::conversation::ContentResidencyVerifier>,
    )> {
        if self.summary.is_some() {
            None
        } else {
            self.stock.inherited_prefix.as_ref()
        }
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_execution_supported(
    stock: &StockExecutor,
    context: &Arc<crate::runtime::TaskContext>,
    journal: &dyn ExecutionJournal,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    work.remaining()?;
    if !context.native_metadata_matches(work) {
        return Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "stock context lacks its original native custody",
        });
    }
    if !journal.supports_native_execution(work)? || !stock.provider.supports_native_model(work)? {
        return Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native stock producer is unavailable",
        });
    }
    if context.durable_task_id() != stock.task.as_ref().map(|(_, task, _)| *task) {
        return Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "stock owner differs from the issued context",
        });
    }
    if let Some((host, task, fence)) = stock.task.as_ref() {
        if !context
            .original_task_binding_matches(*task, fence)
        {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "stock execution owner differs from the issued context",
            });
        }
        if !host.supports_native_execution(work)? {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native stock task owner is unavailable",
            });
        }
    }
    stock
        .context
        .validate(Some(&PreparationIo::from_native_work(work)?))?;
    for (_, policy) in context
        .stock_tool_policy_binding()
        .into_iter()
        .chain(context.stock_tool_policy_overrides())
    {
        policy.supports_native_policy(work)?;
    }
    if let Some(policy) = &stock.policy {
        let Some((_, original)) = context.stock_tool_policy_binding() else {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "stock policy is not the original context policy",
            });
        };
        if !Arc::ptr_eq(policy, original) {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "stock policy is not the original context policy",
            });
        }
    }
    context.validate_stock_scope_with_native_work(
        &stock.tool_scope,
        stock.limits,
        work,
    )?;
    if context.scope().run_limits() != stock.tool_scope.run_limits()
        || context.scope().extensions() != stock.tool_scope.extensions()
    {
        return Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "stock task context differs from execution binding",
        });
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_session<'a>(
    stock: &'a StockExecutor,
    context: &'a Arc<crate::runtime::TaskContext>,
    operation: OperationId,
    io: &'a PreparationIo,
    reconcile: bool,
    interactions: Option<&'a Arc<dyn crate::runtime::InteractionRouter>>,
) -> StockExecution<'a> {
    StockExecution {
        stock,
        native: Some((context, operation)),
        io: Some(io),
        interactions,
        summary: None,
        reconciliation_only: reconcile,
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_progress<'a>(
    stock: &'a StockExecutor,
    context: &'a Arc<crate::runtime::TaskContext>,
    input: &'a TurnInput,
    journal: &'a dyn ExecutionJournal,
    work: &'a acyclic_fs::NativeMaterializationWork,
    reconcile: bool,
    interactions: Option<&'a Arc<dyn crate::runtime::InteractionRouter>>,
) -> Result<crate::NativeFutureBundle<'a, (StockTurnProgress, acyclic_fs::NativeAllocation)>> {
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        io.absorb_native_custody(work.claim(
            std::mem::size_of::<StockTurnProgress>() as u64,
            0,
            acyclic_fs::WorkCounters::default(),
        )?)?;
        let progress = {
            let session = native_session(
                stock,
                context,
                input.operation_id,
                &io,
                reconcile,
                interactions,
            );
            session.execute_progress(input, journal).await?
        };
        let retained = native_progress_owned_bytes(&progress)?;
        work.remaining()?;
        Ok((progress, io.into_native_custody(retained)?))
    })
}

impl StockExecutor {
    /// Summarizes with the same stock admission, accounting and recovery engine.
    pub async fn summarize(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        source: crate::context::Context,
        instruction: ModelContent,
    ) -> Result<crate::context::ContextSummary> {
        StockExecution::ordinary(self)
            .summarize(journal, None, operation, 0, source, instruction)
            .await
    }
    /// Summarizes inside the parent's existing execution journal.
    pub async fn summarize_in_turn(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        source: crate::context::Context,
        instruction: ModelContent,
    ) -> Result<crate::context::ContextSummary> {
        StockExecution::ordinary(self)
            .summarize(
                journal,
                Some(input),
                input.operation_id,
                step,
                source,
                instruction,
            )
            .await
    }
    /// Executes a retained/reconciled/admitted model step through the stock engine.
    pub async fn model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior: &[ModelMessage],
    ) -> Result<Vec<ModelEvent>> {
        StockExecution::ordinary(self)
            .model_step(journal, input, step, prior)
            .await
    }
    /// Freezes a completed tool call prefix without dispatching effects.
    pub async fn completed_tool_prefix(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        call: Option<&str>,
    ) -> Result<crate::model::PreparedModelRequest> {
        StockExecution::ordinary(self)
            .completed_tool_prefix(journal, input, step, call)
            .await
    }
}

impl StockExecutor {
    pub(crate) async fn execute_progress(
        &self,
        input: TurnInput,
        journal: &dyn ExecutionJournal,
    ) -> Result<StockTurnProgress> {
        let span = obs_span!(
            "acyclic.harness.executor.execute",
            operation_id = format_args!("{}", input.operation_id)
        );
        traced(
            span,
            StockExecution::ordinary(self).execute_progress(&input, journal),
        )
        .await
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl StockExecutor {
    /// Audits the native callback boundaries without cloning configuration or boxing work.
    pub(crate) fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        work.remaining()?;
        if !self.provider.supports_native_model(work)? {
            return Ok(false);
        }
        self.context
            .validate(Some(&PreparationIo::from_native_work(work)?))?;
        if let Some((host, _, _)) = &self.task
            && !host.supports_native_execution(work)?
        {
            return Ok(false);
        }
        if let Some(policy) = &self.policy {
            policy.supports_native_policy(work)?;
        }
        Ok(true)
    }
}
