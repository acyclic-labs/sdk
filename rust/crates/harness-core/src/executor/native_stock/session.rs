use super::*;

pub(in crate::executor) struct StockExecution<'a> {
    pub(in crate::executor) stock: &'a StockExecutor,
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
        acyclic_stream::trace::observe_value(
            acyclic_stream::trace::Request {
                name: "harness.journal_writer_lock",
                mechanism: acyclic_stream::trace::Mechanism::Lock,
                parent: None,
                guarantee: Some("serialized_journal_read_tail_commit"),
            },
            self.mutex().lock(),
            |_| Some((Some(0), acyclic_stream::trace::Outcome::Succeeded)),
        )
        .await
    }
}

/// The turn's compaction policy, resolved once from the original IO, with the
/// private summary authority that resolution validated.
pub(in crate::executor) struct ResolvedCompaction<'a> {
    pub(in crate::executor) policy: &'a crate::context::CompactionPolicy,
    pub(in crate::executor) destination: Option<crate::runtime::SummaryDestination<'a>>,
}

impl<'a> StockExecution<'a> {
    pub(in crate::executor) fn ordinary(stock: &'a StockExecutor) -> Self {
        Self {
            stock,
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
        Ok(self.compaction()?.policy)
    }
    pub(in crate::executor) fn compaction(&self) -> Result<&ResolvedCompaction<'a>> {
        self.compaction
            .get_or_try_init(|| self.resolve_compaction())
    }
    fn resolve_compaction(&self) -> Result<ResolvedCompaction<'a>> {
        use crate::context::CompactionPolicy;
        static CAPACITY: std::sync::LazyLock<CompactionPolicy> =
            std::sync::LazyLock::new(CompactionPolicy::default);
        let stock: &'a StockExecutor = self.stock;
        let disabled = || ResolvedCompaction {
            policy: &CompactionPolicy::Disabled,
            destination: None,
        };
        Ok(match (&self.summary, &stock.compaction, self.context) {
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
                    policy,
                    destination,
                }
            }
            (None, Some(CompactionPolicy::Disabled), _) => disabled(),
            (None, None, Some((context, _))) => {
                let destination = context.summary_destination(self.io)?;
                ResolvedCompaction {
                    policy: if destination.writer().is_some() {
                        &CAPACITY
                    } else {
                        &CompactionPolicy::Disabled
                    },
                    destination: Some(destination),
                }
            }
            (None, None, None) => ResolvedCompaction {
                policy: &CAPACITY,
                destination: None,
            },
        })
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

#[cfg(test)]
mod trace_tests {
    use super::JournalWrites;
    use acyclic_stream::trace::{Mechanism, Observation, Outcome, Recorder, TraceQuery};
    use std::sync::Arc;

    #[tokio::test]
    async fn borrowed_journal_writer_observes_original_lock_and_guard()
    -> Result<(), Box<dyn std::error::Error>> {
        let writers = JournalWrites::default();
        let borrowed = writers.borrowed();
        let held = writers.lock().await;
        let trace = acyclic_stream::Stream::new(
            Arc::new(acyclic_stream::MemoryStream::default()),
            "agents/worker/trace",
        )?;
        let recorder = Arc::new(Recorder::new(trace.clone(), [25; 16]));
        let root = recorder
            .start(
                "request",
                Mechanism::ProviderCall,
                None,
                Some("original_request"),
            )
            .await?
            .into_owned();
        let parent = root.id();
        let mut waiting = Box::pin(root.within(borrowed.lock()));
        assert!(futures::poll!(&mut waiting).is_pending());
        drop(held);
        let guard = waiting.await;
        assert!(
            writers.mutex().try_lock().is_err(),
            "observation replaced the original writer guard"
        );
        drop(guard);
        assert!(writers.mutex().try_lock().is_ok());
        let _terminal = root.finish_submitted(None, Outcome::Succeeded)?;
        let mut query = TraceQuery::new(
            trace.follow(0, Default::default()).await?,
            Default::default(),
        );
        let mut completed = 0;
        let mut lock = false;
        while completed < 2 {
            if let Some(Observation::Completed {
                start,
                result,
                bytes,
                ..
            }) = query.next().await?
            {
                completed += 1;
                if start.name == "harness.journal_writer_lock" {
                    assert_eq!(start.parent, Some(parent));
                    assert_eq!(start.mechanism, Mechanism::Lock);
                    assert_eq!(
                        start.guarantee.as_deref(),
                        Some("serialized_journal_read_tail_commit")
                    );
                    assert_eq!(result, Outcome::Succeeded);
                    assert_eq!(bytes, Some(0));
                    lock = true;
                }
            }
        }
        assert!(lock);
        assert_eq!(query.totals()[&Mechanism::Lock].completed, 1);
        drop(writers.lock().await);
        assert_eq!(
            trace.tail().await?,
            4,
            "unscoped lock acquired diagnostic work"
        );
        Ok(())
    }
}
