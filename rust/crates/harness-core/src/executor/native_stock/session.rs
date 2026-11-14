use super::*;

pub(in crate::executor) struct StockExecution<'a> {
    /// The immutable prefix composition, independent of per-step bindings.
    pub(in crate::executor) stock: &'a StockExecutor,
    pub(in crate::executor) selected: Option<Arc<StepBinding>>,
    pub(in crate::executor) configuration: Option<&'a Arc<dyn StepConfiguration>>,
    pub(in crate::executor) context: Option<(&'a Arc<crate::runtime::TaskContext>, OperationId)>,
    pub(in crate::executor) io: Option<&'a PreparationIo>,
    pub(in crate::executor) summary: Option<ContextPipeline>,
    pub(in crate::executor) single_request: bool,
    /// Serializes journal transitions shared by response and summary producers.
    pub(in crate::executor) journal_writes: JournalWrites<'a>,
    /// Resolved on first use, after `context` and `io` are fixed.
    pub(in crate::executor) compaction: once_cell::sync::OnceCell<ResolvedCompaction<'a>>,
    pub(in crate::executor) records:
        tokio::sync::Mutex<crate::executor::record_index::RecordIndex<'a>>,
    pub(in crate::executor) working_tail:
        tokio::sync::Mutex<crate::executor::working_tail::TailCache<'a>>,
    pub(in crate::executor) request_cache:
        tokio::sync::Mutex<crate::executor::retained_request::RequestCache<'a>>,
    pub(in crate::executor) content_admission: crate::executor::ExecutionJournalCacheId,
    pub(in crate::executor) review: crate::executor::context_review::Review<'a>,
}

/// One writer entrance for an execution and its borrowed summary sessions.
/// The guard covers a read-tail/commit transition, never provider or tool execution.
pub(in crate::executor) enum JournalWrites<'a> {
    Owned(tokio::sync::Mutex<()>),
    Borrowed(&'a tokio::sync::Mutex<()>),
}

impl Default for JournalWrites<'_> {
    fn default() -> Self {
        Self::Owned(tokio::sync::Mutex::new(()))
    }
}

impl JournalWrites<'_> {
    fn mutex(&self) -> &tokio::sync::Mutex<()> {
        match self {
            Self::Owned(mutex) => mutex,
            Self::Borrowed(mutex) => mutex,
        }
    }

    pub(in crate::executor) fn borrowed(&self) -> JournalWrites<'_> {
        JournalWrites::Borrowed(self.mutex())
    }

    pub(in crate::executor) async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.mutex().lock().await
    }
}

/// The turn's compaction policy, resolved once from the original IO, with the
/// private summary authority that resolution validated.
pub(in crate::executor) struct ResolvedCompaction<'a> {
    pub(in crate::executor) policy: crate::context::CompactionPolicy,
    pub(in crate::executor) destination: Option<crate::runtime::SummaryDestination<'a>>,
}

impl<'a> StockExecution<'a> {
    pub(in crate::executor) fn selected_model(&self) -> &Model {
        self.selected
            .as_ref()
            .map_or(&self.stock.model, |selected| &selected.model)
    }
    pub(in crate::executor) fn selected_provider(&self) -> &Arc<dyn ModelProvider> {
        self.selected
            .as_ref()
            .map_or(&self.stock.provider, |selected| &selected.provider)
    }
    pub(in crate::executor) fn selected_tools(&self) -> &ToolRegistry {
        self.selected
            .as_ref()
            .map_or(&self.stock.tools, |selected| &selected.tools)
    }
    pub(in crate::executor) fn selected_sampling(&self) -> Option<crate::model::ModelSampling> {
        self.selected
            .as_ref()
            .map_or(self.stock.sampling, |selected| selected.sampling)
    }
    pub(in crate::executor) fn selected_sampling_cache(
        &self,
    ) -> &once_cell::sync::OnceCell<crate::model::ModelSampling> {
        self.selected
            .as_ref()
            .map_or(&self.stock.resolved_sampling, |selected| {
                &selected.resolved_sampling
            })
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::executor) fn selected_hooks(&self) -> &Option<Arc<crate::hooks::BoundHooks>> {
        self.selected
            .as_ref()
            .map_or(&self.stock.hooks, |selected| &selected.hooks)
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::executor) fn selected_after_hooks(&self) -> &[crate::plugins::AfterHook] {
        self.selected
            .as_ref()
            .map_or(&self.stock.plugin_set, |selected| &selected.plugin_set)
            .as_ref()
            .map_or(&[], |plugins| plugins.after.as_slice())
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::executor) fn check_prompt_admission(&self) -> Result<()> {
        let block = self
            .selected
            .as_ref()
            .map_or(&self.stock.prompt_block, |selected| &selected.prompt_block);
        match block {
            Some(reason) => Err(Error::Invalid(stock_string_copy(reason, self.io)?)),
            None => Ok(()),
        }
    }
    pub(in crate::executor) fn ordinary(stock: &'a StockExecutor) -> Self {
        Self {
            stock,
            selected: None,
            configuration: stock.step_configuration.as_ref(),
            context: stock
                .task_context
                .as_ref()
                .map(|(context, operation)| (context, *operation)),
            io: None,
            summary: None,
            single_request: false,
            journal_writes: JournalWrites::default(),
            compaction: once_cell::sync::OnceCell::new(),
            records: tokio::sync::Mutex::new(Default::default()),
            working_tail: tokio::sync::Mutex::new(Default::default()),
            request_cache: tokio::sync::Mutex::new(Default::default()),
            content_admission: Default::default(),
            review: Default::default(),
        }
    }
    pub(in crate::executor) fn preparing<'b>(
        &'b self,
        io: Option<&'b PreparationIo>,
    ) -> StockExecution<'b> {
        StockExecution {
            stock: self.stock,
            selected: self.selected.clone(),
            configuration: self.configuration,
            context: self.context,
            io,
            summary: None,
            single_request: false,
            journal_writes: self.journal_writes.borrowed(),
            compaction: once_cell::sync::OnceCell::new(),
            records: tokio::sync::Mutex::new(Default::default()),
            working_tail: tokio::sync::Mutex::new(Default::default()),
            request_cache: tokio::sync::Mutex::new(Default::default()),
            content_admission: Default::default(),
            review: Default::default(),
        }
    }

    pub(in crate::executor) fn native_work(&self) -> acyclic_fs::NativeWork<'_> {
        { self.io.and_then(PreparationIo::original_native_work) }
    }
    pub(in crate::executor) fn original_context(&self) -> Option<&crate::runtime::TaskContext> {
        self.context.map(|(context, _)| context.as_ref())
    }
    pub(in crate::executor) fn context_pipeline(&self) -> &ContextPipeline {
        self.summary
            .as_ref()
            .map_or(&self.stock.context, |pipeline| pipeline)
    }
    pub(in crate::executor) fn compaction_policy(
        &self,
    ) -> Result<&crate::context::CompactionPolicy> {
        Ok(&self.compaction()?.policy)
    }
    pub(in crate::executor) fn compaction(&self) -> Result<&ResolvedCompaction<'a>> {
        self.compaction
            .get_or_try_init(|| self.resolve_compaction())
    }
    fn resolve_compaction(&self) -> Result<ResolvedCompaction<'a>> {
        use crate::context::CompactionPolicy;
        let disabled = || ResolvedCompaction {
            policy: CompactionPolicy::Disabled,
            destination: None,
        };
        Ok(
            match (&self.summary, &self.stock.compaction, self.context) {
                (Some(_), _, _) => disabled(),
                (None, Some(policy @ CompactionPolicy::Capacity(_)), _) => {
                    let destination = self
                        .context
                        .map(|(context, _)| context.summary_destination(self.io))
                        .transpose()?;
                    if let Some(destination) = destination {
                        destination.require(self.io)?;
                    }
                    ResolvedCompaction {
                        policy: policy.clone(),
                        destination,
                    }
                }
                (None, Some(CompactionPolicy::Disabled), _) => disabled(),
                (None, None, Some((context, _))) => {
                    let destination = context.summary_destination(self.io)?;
                    ResolvedCompaction {
                        policy: if destination.writer().is_some() {
                            CompactionPolicy::default()
                        } else {
                            CompactionPolicy::Disabled
                        },
                        destination: Some(destination),
                    }
                }
                (None, None, None) => ResolvedCompaction {
                    policy: CompactionPolicy::default(),
                    destination: None,
                },
            },
        )
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

impl StockExecutor {
    /// Summarizes with the same stock admission, accounting and recovery engine.
    pub async fn summarize(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        source: crate::context::Context,
        instruction: ModelContent,
    ) -> Result<crate::context::ContextSummary> {
        let execution = journal.claim_execution(operation, None)?;
        let phase = &mut DispatchPhase::new();
        let result = StockExecution::ordinary(self)
            .summarize(journal, None, operation, 0, (source, instruction), phase)
            .await
            .map(|(summary, _)| summary)?;
        execution.release();
        Ok(result)
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
        let ownership = journal.claim_execution(input.operation_id, None)?;
        let phase = &mut DispatchPhase::new();
        let execution = StockExecution::ordinary(self);
        execution.validate_turn_input(journal, input).await?;
        let result = execution
            .summarize(
                journal,
                Some(input),
                input.operation_id,
                step,
                (source, instruction),
                phase,
            )
            .await
            .map(|(summary, _)| summary)?;
        ownership.release();
        Ok(result)
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
    ) -> Result<ExecutionProgress<TurnOutput>> {
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

#[cfg(not(target_arch = "wasm32"))]
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
        if let Some(policy) = &self.policy {
            policy.identity_with_native_work(work)?;
        }
        Ok(true)
    }
}
