//! Model-owned rewrite decisions, driven alongside the worker without a task.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify};

const INSTRUCTION: &str = "You manage this worker's context outside its turns. Decide using the supplied original usage and the work still needed, including the cost of another summary, cache rebuild and repeated cached input, and the latency of attending to a growing history. Do not wait for the context window to fill. Preserve instructions, facts, decisions, constraints, evidence references and unfinished work. Return only JSON with exactly three fields: version (1), summary (a concise replacement for the supplied history, or null when keeping it is more economical) and review_at_input_tokens (a positive input-token count below the advertised capacity at which to reconsider). This is your budget, not the model capacity. When keeping history, choose a count greater than the current reported input. When summarising, choose the next review count for the smaller replacement context. Never remove facts required by ongoing work. The validated JSON decision becomes the durable summary; include all retained task information inside summary.";

#[derive(Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct Decision {
    version: u32,
    summary: Option<String>,
    review_at_input_tokens: u64,
}

impl PreparationRecord for Decision {}

/// The reason a re-asked review's previous decision was rejected.
struct Rejection<'r>(Option<&'r str>);

impl std::fmt::Display for Rejection<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(reason) => write!(
                formatter,
                "\nYour previous decision was rejected: {reason}. Return exactly one JSON object and escape every control character inside its strings (a newline as \\n)."
            ),
            None => Ok(()),
        }
    }
}

#[derive(Clone, Copy)]
struct Observation {
    tokens: ModelTokenUsage,
    input_tokens: u64,
    latency_ns: Option<u64>,
}

struct Job<'a> {
    step: u32,
    source: crate::context::Context,
    selected: Option<Arc<StepBinding>>,
    observation: Observation,
    _hold: Option<crate::conversation::ContentHold<'a>>,
}

struct Receipt<'a> {
    summary: crate::context::ContextSummary,
    files: Option<FileRef>,
    _files_hold: Option<crate::conversation::ContentHold<'a>>,
    _hold: Option<crate::conversation::ContentHold<'a>>,
}

#[derive(Default)]
struct State<'a> {
    review_at: u64,
    /// Why the last decision was rejected, owed one re-ask at the next boundary.
    rejected: Option<String>,
    queued: Option<Job<'a>>,
    active: bool,
    ready: Option<Result<Option<Receipt<'a>>>>,
}

pub(in crate::executor) enum Review<'a> {
    Local(Background<'a>),
    Shared(Arc<Background<'a>>),
}

impl Default for Review<'_> {
    fn default() -> Self {
        Self::Local(Background::default())
    }
}

impl<'a> std::ops::Deref for Review<'a> {
    type Target = Background<'a>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Local(background) => background,
            Self::Shared(background) => background,
        }
    }
}

impl<'a> Background<'a> {
    pub(super) fn shared(io: Option<&PreparationIo>) -> Result<Arc<Self>> {
        let storage = io.map(PreparationIo::scoped).transpose()?;
        let bytes = acyclic_fs::shared_allocation_bytes::<Self>().map_err(|_| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "context review storage overflows",
            )
        })?;
        crate::executor::with_history_io(storage.as_ref(), |work| -> Result<()> {
            if let Some(work) = work.as_deref_mut() {
                work.retain_owned(bytes)?;
                work.allocation_attempt(bytes)?;
            }
            Ok(())
        })?;
        let custody = storage
            .as_ref()
            .map(PreparationIo::take_all_owned_custody)
            .transpose()?;
        Ok(Arc::new(Self {
            _custody: custody,
            ..Self::default()
        }))
    }
}
#[derive(Default)]
pub(in crate::executor) struct Background<'a> {
    enabled: AtomicBool,
    stopped: AtomicBool,
    paused: AtomicBool,
    state: Mutex<State<'a>>,
    pub(super) requested: Notify,
    pub(super) completed: Notify,
    stop: Notify,
    _custody: Option<crate::filesystem::PreparationCustody>,
}

impl<'a> StockExecution<'a> {
    pub(super) async fn with_context_review<T>(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        worker: impl std::future::Future<Output = Result<ExecutionProgress<T>>>,
    ) -> Result<ExecutionProgress<T>> {
        if !self.stock.context_management {
            return worker.await;
        }
        self.review.enabled.store(true, Ordering::Release);
        self.review.stopped.store(false, Ordering::Release);
        self.review.paused.store(false, Ordering::Release);
        let worker = async {
            let result = worker.await;
            self.review.paused.store(
                matches!(result, Ok(ExecutionProgress::Pending(_))),
                Ordering::Release,
            );
            self.review.stopped.store(true, Ordering::Release);
            self.review.stop.notify_one();
            result
        };
        let (result, cleanup) = futures::join!(worker, self.context_review_service(journal, input));
        self.review.enabled.store(false, Ordering::Release);
        *self.review.state.lock().await = State::default();
        cleanup?;
        result
    }

    /// Called for original and replayed worker completions. No conservative
    /// byte bound, timer or capacity fraction can request a context review.
    pub(super) async fn request_context_review(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior: &[ModelMessage],
        metadata: &Value,
    ) -> Result<()> {
        if !self.review.enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        if input
            .max_steps
            .is_some_and(|maximum| step >= maximum.saturating_sub(1))
        {
            return Ok(());
        }
        let usage = ModelTokenUsage::reported(metadata);
        let Some(input_tokens) = self
            .selected_provider()
            .reported_input_tokens(self.selected_model(), metadata)?
        else {
            return Ok(());
        };
        let mut state = self.review.state.lock().await;
        if state.active || input_tokens < state.review_at {
            return Ok(());
        }
        // Replayed completions restore their original decision before another
        // review may be admitted. Timing of the borrowed service is not a
        // durable scheduler and must not choose a different source on restart.
        let (_, recorded) = self
            .model_records(journal, input.operation_id, step, ModelPurpose::Summary)
            .await?;
        let completed = recorded.iter().any(|record| {
            matches!(
                record.event,
                ExecutionEvent::Model { usage: Some(_), .. }
                    | ExecutionEvent::ModelInline { usage: Some(_), .. }
            )
        });
        if recorded.is_empty()
            && let Some(next) = step.checked_add(1)
            && !self
                .step_records(journal, input.operation_id, next, false, Some(1), |event| {
                    matches!(
                        event,
                        ExecutionEvent::ModelStarted {
                            purpose: ModelPurpose::Response,
                            ..
                        }
                    )
                })
                .await?
                .1
                .is_empty()
        {
            // The worker already passed this boundary without admitting a
            // review. Replay must preserve that original scheduling choice,
            // even when an earlier review now completes synchronously.
            return Ok(());
        }
        if state.ready.is_some() && !completed {
            return Ok(());
        }
        if recorded
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ModelStopped { .. }))
        {
            return Ok(());
        }
        let crate::context::CompactionPolicy::Capacity(policy) = self.compaction_policy()? else {
            return Ok(());
        };
        let (_, response) = self
            .model_records(journal, input.operation_id, step, ModelPurpose::Response)
            .await?;
        if response
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
        {
            // This request already selected a smaller working base. Review its
            // successor, not the superseded history preceding this boundary.
            return Ok(());
        }
        let mut observation = Observation {
            tokens: usage,
            input_tokens,
            latency_ns: None,
        };
        if let Some(reference) = response
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                ExecutionEvent::Model { usage, .. } | ExecutionEvent::ModelInline { usage, .. } => {
                    usage.as_ref()
                }
                _ => None,
            })
        {
            let (original, hold) =
                load_json::<ModelUsageRecord>(journal, reference, self.io).await?;
            model_usage::validate_metadata(
                &original,
                input.operation_id,
                step,
                ModelPurpose::Response,
                metadata,
                self.io,
            )?;
            observation.latency_ns = original.latency_ns;
            drop(original);
            drop(hold);
        }
        let (_, base, _) = self.response_base(journal, input, step, prior).await?;
        let budget = with_history_io(self.io, |work| {
            policy.try_projection_budget(&base, None, base.messages.len().saturating_sub(1), work)
        })?;
        let Some((through, _)) = budget else {
            return Ok(());
        };
        let (source, bytes) = with_history_io(self.io, |work| {
            crate::context::clone_context(&base, Some(through), work)
        })?;
        #[cfg(all(test, not(target_arch = "wasm32")))]
        acyclic_test_support::differential::record((
            "context review source messages",
            through as u64,
        ));
        let hold = self.io.map(|io| io.guard_owned(bytes)).transpose()?;
        let job = Job {
            step,
            source,
            selected: self.selected.clone(),
            observation,
            _hold: hold,
        };
        if completed {
            drop(state);
            let mut phase = DispatchPhase::new();
            let ready = self
                .evaluate_context_review(
                    journal,
                    input,
                    (job.source, job.selected),
                    step,
                    observation,
                    &mut phase,
                )
                .await;
            drop(job._hold);
            self.review.state.lock().await.ready = Some(ready);
            return Ok(());
        }
        state.queued = Some(job);
        state.active = true;
        drop(state);
        self.review.requested.notify_one();
        Ok(())
    }

    /// A real response boundary may consume one completed decision. It never
    /// waits for housekeeping. The capacity backstop remains independent.
    pub(super) async fn review_response(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32, &[ModelMessage]),
        projection: &ResponseProjection,
        capacity: crate::context::ModelContextCapacity,
        at_capacity: bool,
    ) -> Result<Option<crate::model::PreparedModelRequest>> {
        if !self.review.enabled.load(Ordering::Acquire) {
            return Ok(None);
        }
        let ready = loop {
            let completed = self.review.completed.notified();
            let mut state = self.review.state.lock().await;
            if !at_capacity || !state.active {
                break state.ready.take();
            }
            drop(state);
            completed.await;
        };
        let Some(Some(receipt)) = ready.transpose()? else {
            return Ok(None);
        };
        let (input, step, prior) = turn;
        let (_, source, _) = self.response_base(journal, input, step, prior).await?;
        let through = receipt.summary.source_messages as usize;
        if through >= source.messages.len()
            || crate::context::prefix_digest(&source, through, self.io)?
                != receipt.summary.source_digest
        {
            // Steering or an earlier capacity cut invalidated the source.
            return Ok(None);
        }
        let crate::context::CompactionPolicy::Capacity(policy) = self.compaction_policy()? else {
            return Ok(None);
        };
        let (planned, maximum) =
            crate::context::plan_compaction_budget(policy, &source, None, through, self.io)?;
        if planned != through {
            return Ok(None);
        }
        if let Some(hold) = receipt._hold {
            hold.into_retained()?;
        }
        let files = receipt.files;
        let _files_hold = receipt._files_hold;
        let (retained, proof) =
            crate::context::compact_projection(&source, maximum, Some(receipt.summary), self.io)?;
        let (compaction, working) = self
            .stage_working_cut(
                journal,
                working_tail::WorkingCompaction {
                    input,
                    step,
                    prior: prior.len(),
                    source: &source,
                    retained: &retained,
                    proof: &proof,
                },
            )
            .await?;
        let (context, mut request) = self
            .prepare_compacted_request(input, step, retained, true)
            .await?;
        let compacted = ResponseProjection {
            context,
            steering: projection.steering.clone(),
            canonical: None,
            checkpoint: None,
            working: Some(working),
            accounting: None,
        };
        if self
            .finish_compaction(
                journal,
                (input, step),
                &compacted,
                compaction,
                (
                    capacity,
                    files
                        .as_ref()
                        .map(|files| context_files::RewritePublication {
                            files,
                            proof: &proof,
                        }),
                ),
                &mut request,
            )
            .await?
        {
            return Ok(Some(request));
        }
        Err(stock_boundary(
            self.io,
            crate::wire::ErrorCode::Invalid,
            "context review replacement exceeds model capacity",
        ))
    }

    async fn evaluate_context_review(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        source: (crate::context::Context, Option<Arc<StepBinding>>),
        step: u32,
        observation: Observation,
        phase: &mut DispatchPhase,
    ) -> Result<Option<Receipt<'a>>> {
        let (source, selected) = source;
        let files = self.rewrite_files(journal, (input, step)).await?;
        let files_hold = files
            .as_ref()
            .zip(self.io)
            .map(|(file, io)| io.guard_owned(file.logical_owned_bytes()?))
            .transpose()?;
        let scope = crate::filesystem::PreparationScope::new(self.io)?;
        let mut session = self.preparing(scope.io());
        session.selected = selected;
        session.configuration = None;
        let capacity = session.model_capacity()?;
        let rejected = self.review.state.lock().await.rejected.take();
        let instruction = with_history_io(session.io, |work| {
            preparation_format(
                format_args!(
                    "{INSTRUCTION}\nSelected model: {}\nCapacity: {}\nComplete reported input tokens: {}\nOriginal billing counters: {:?}\nOriginal call latency_ns: {:?}\nPrices not supplied are unknown; do not invent prices.{}",
                    session.selected_model().name,
                    capacity.context_tokens,
                    observation.input_tokens,
                    observation.tokens,
                    observation.latency_ns,
                    Rejection(rejected.as_deref()),
                ),
                work,
            )
        })?;
        let instruction = ModelContent::Text(instruction);
        let digest = crate::context::prefix_digest(&source, source.messages.len(), session.io)?;
        let end = match session
            .manager_window(
                journal,
                (input, step),
                &source,
                (source.messages.len(), Some(&instruction)),
                (0, None, digest),
            )
            .await
        {
            Ok(end) => end,
            Err(Error::ModelContextOverflow) => {
                // No paid admission occurred. An irreducible review must not
                // prevent ordinary work or the independent capacity backstop.
                drop(instruction);
                drop(session);
                scope.finish(0)?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let material = session.manager_material(&source, 0..end, None, Some(&instruction))?;
        let output = session
            .summarize_output(
                journal,
                Some(input),
                input.operation_id,
                step,
                material,
                phase,
                ModelPurpose::Summary,
            )
            .await?;
        let (summary, text) = match output {
            Ok(output) => output,
            Err(defect) => {
                drop(instruction);
                drop(session);
                scope.finish(0)?;
                return self
                    .reject_decision(defect.to_owned(), rejected.is_some(), capacity)
                    .await;
            }
        };
        let retained = summary
            .output
            .logical_owned_bytes()?
            .checked_add(text.capacity() as u64)
            .ok_or_else(|| {
                stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Invalid,
                    "review ownership overflows",
                )
            })?;
        drop(instruction);
        drop(session);
        scope.finish(retained)?;
        let hold = self
            .io
            .map(|io| io.guard_owned(summary.output.logical_owned_bytes()?))
            .transpose()?;
        let decoded = match self.io {
            Some(io) => io
                .decode(
                    std::marker::PhantomData::<Decision>,
                    text.as_bytes(),
                    crate::wire::ErrorCode::Invalid,
                )
                .map(|(decision, hold)| (decision, Some(hold)))
                .map_err(|error| error.to_string()),
            None => crate::contract::json_from_slice::<Decision>(text.as_bytes())
                .map(|decision| (decision, None))
                .map_err(|error| error.to_string()),
        };
        stock_retire_text(text, self.io)?;
        let (decision, decision_hold) = match decoded {
            Ok(decoded) => decoded,
            Err(error) => {
                drop(summary);
                drop(hold);
                return self
                    .reject_decision(
                        format!("it is not one JSON object of the required shape ({error})"),
                        rejected.is_some(),
                        capacity,
                    )
                    .await;
            }
        };
        let valid = decision.version == 1
            && decision.review_at_input_tokens > 0
            && decision.review_at_input_tokens < u64::from(capacity.context_tokens)
            && match &decision.summary {
                Some(text) => !text.trim().is_empty(),
                None => decision.review_at_input_tokens > observation.input_tokens,
            };
        let (rewrite, next) = (decision.summary.is_some(), decision.review_at_input_tokens);
        drop(decision);
        drop(decision_hold);
        if !valid {
            drop(summary);
            drop(hold);
            return self
                .reject_decision(
                    "version must be 1, review_at_input_tokens must be positive and below the capacity, and summary must be nonempty, or null with review_at_input_tokens above the current input".to_owned(),
                    rejected.is_some(),
                    capacity,
                )
                .await;
        }
        self.review.state.lock().await.review_at = next;
        if !rewrite {
            drop(summary);
            drop(hold);
            return Ok(None);
        }
        // Reuse the model's original output and provenance. A second copy of
        // the summary text would add storage and another publication mechanism.
        Ok(Some(Receipt {
            summary,
            files,
            _files_hold: files_hold,
            _hold: hold,
        }))
    }

    /// A malformed, empty or invalid decision is the reviewer's defect, never
    /// the worker's: the context stays unreviewed. The first rejection is
    /// re-asked with its reason at the next response boundary; a second defers
    /// review until the input reaches capacity, where the independent capacity
    /// backstop compacts.
    async fn reject_decision(
        &self,
        reason: String,
        reasked: bool,
        capacity: crate::context::ModelContextCapacity,
    ) -> Result<Option<Receipt<'a>>> {
        let mut state = self.review.state.lock().await;
        if reasked {
            state.review_at = u64::from(capacity.context_tokens);
        } else {
            state.rejected = Some(reason);
        }
        Ok(None)
    }

    async fn context_review_service(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<()> {
        loop {
            let requested = self.review.requested.notified();
            let stopped = self.context_review_stopped();
            if self.review.stopped.load(Ordering::Acquire) {
                return Ok(());
            }
            let job = self.review.state.lock().await.queued.take();
            let Some(job) = job else {
                futures::pin_mut!(requested, stopped);
                let _ = futures::future::select(requested, stopped).await;
                continue;
            };
            let step = job.step;
            let mut phase = DispatchPhase::new();
            let review = self.evaluate_context_review(
                journal,
                input,
                (job.source, job.selected),
                step,
                job.observation,
                &mut phase,
            );
            let result = {
                futures::pin_mut!(review, stopped);
                match futures::future::select(review, stopped).await {
                    futures::future::Either::Left((result, _)) => Some(result),
                    futures::future::Either::Right(_) => None,
                }
            };
            drop(job._hold);
            let Some(result) = result else {
                return self
                    .stop_context_review(journal, input.operation_id, step)
                    .await;
            };
            let cleanup = if result.is_err() {
                self.stop_context_review(journal, input.operation_id, step)
                    .await
            } else {
                Ok(())
            };
            let mut state = self.review.state.lock().await;
            state.active = false;
            state.ready = Some(result);
            drop(state);
            self.review.completed.notify_one();
            // A cleanup refusal must not strand a worker awaiting this result.
            cleanup?;
        }
    }

    async fn context_review_stopped(&self) {
        loop {
            let stopped = self.review.stop.notified();
            if self.review.stopped.load(Ordering::Acquire) {
                return;
            }
            // A previous scoped run can leave a notification permit. Only
            // this run's terminal state authorises cancelling its producer.
            stopped.await;
        }
    }

    async fn stop_context_review(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
    ) -> Result<()> {
        let completion = phase::PreparedPhase::new(journal, self.io)?;
        completion
            .run(async {
                let _writer = self.journal_writes.lock().await;
                let (tail, records) = self
                    .model_records(journal, operation, step, ModelPurpose::Summary)
                    .await?;
                if records
                    .iter()
                    .any(|r| matches!(r.event, ExecutionEvent::ModelStarted { .. }))
                    && !records.iter().any(|r| {
                        matches!(
                            r.event,
                            ExecutionEvent::Model { usage: Some(_), .. }
                                | ExecutionEvent::ModelInline { usage: Some(_), .. }
                                | ExecutionEvent::ModelStopped { .. }
                        )
                    })
                {
                    let key = with_history_io(self.io, |work| {
                        preparation_format(format_args!("summary:{step}:stopped"), work)
                    })?;
                    if !journal
                        .append_if_tail(
                            operation,
                            tail,
                            &key,
                            &ExecutionEvent::ModelStopped {
                                step,
                                purpose: ModelPurpose::Summary,
                                worker_paused: self.review.paused.load(Ordering::Acquire),
                            },
                            self.io,
                        )?
                        .await?
                    {
                        return Err(Error::Indeterminate(operation));
                    }
                }
                Ok(())
            })
            .await
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_cleanup_wakes_worker_at_capacity() -> Result<()> {
        let provider = Arc::new(
            crate::test_support::ScriptedModelProvider::new([]).with_context_capacity(
                crate::context::ModelContextCapacity {
                    context_tokens: 8192,
                    output_tokens: Some(512),
                    output_shares_context: false,
                },
            ),
        );
        let stock = StockExecutor::new(
            Model::new("fixture".into(), "selected".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_automatic_compaction()?;
        let storage =
            crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 8192).await?;
        let journal = storage.journal();
        let input = TurnInput {
            operation_id: OperationId::new(),
            max_steps: None,
            input: ModelContent::Text("Continue.".into()),
            selected_context: None,
        };
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024),
            &cancellation,
        )?;
        let mut execution = StockExecution::ordinary(&stock);
        execution.io = Some(&io);
        // Resume the service at its admitted-job boundary, then cancel the
        // original work shared by evaluation and durable cleanup.
        {
            let mut state = execution.review.state.lock().await;
            state.active = true;
            state.queued = Some(Job {
                step: 0,
                source: crate::context::Context::default(),
                selected: None,
                observation: Observation {
                    tokens: ModelTokenUsage::default(),
                    input_tokens: 4096,
                    latency_ns: None,
                },
                _hold: None,
            });
        }
        cancellation.cancel();
        assert!(
            execution
                .stop_context_review(journal.as_ref(), input.operation_id, 0)
                .await
                .is_err()
        );
        let projection = ResponseProjection {
            context: crate::context::Context::default(),
            steering: None,
            canonical: None,
            checkpoint: None,
            working: None,
            accounting: None,
        };
        let worker = async {
            let waiting = execution.review_response(
                journal.as_ref(),
                (&input, 1, &[]),
                &projection,
                crate::context::ModelContextCapacity {
                    context_tokens: 8192,
                    output_tokens: Some(512),
                    output_shares_context: false,
                },
                true,
            );
            futures::pin_mut!(waiting);
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            assert!(waiting.await.is_err());
            Ok(ExecutionProgress::Ready(()))
        };
        assert!(
            execution
                .with_context_review(journal.as_ref(), &input, worker)
                .await
                .is_err()
        );
        assert_eq!(provider.request_count(), 0);
        assert!(!execution.review.state.lock().await.active);
        Ok(())
    }
}
