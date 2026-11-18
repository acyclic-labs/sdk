//! Model-owned rewrite decisions, driven alongside the worker without a task.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify};

const INSTRUCTION: &str = "You manage this worker's context outside its turns. Decide using the supplied original usage and the work still needed, including the cost of another summary, cache rebuild and repeated cached input, and the latency of attending to a growing history. Do not wait for the context window to fill. Preserve instructions, facts, decisions, constraints, evidence references and unfinished work. Return only JSON with exactly three fields: version (1), summary (a concise replacement for the supplied history, or null when keeping it is more economical) and review_at_input_tokens (a positive input-token count below the advertised capacity at which to reconsider). This is your budget, not the model capacity. When keeping history, choose a count greater than the current reported input. When summarising, choose the next review count for the smaller replacement context. Never remove facts required by ongoing work. The validated JSON decision becomes the durable summary; include all retained task information inside summary.";

#[derive(Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct Decision {
    version: u32,
    #[serde(deserialize_with = "Option::deserialize")]
    #[heap_layout(as = Option<String>)]
    summary: Option<String>,
    review_at_input_tokens: u64,
}

impl PreparationRecord for Decision {}

/// A bounded rejection reason that never copies the reply: serde's data
/// errors quote field names and values, so only the category and position
/// are kept.
fn decode_rejection(error: &serde_json::Error) -> String {
    let kind = match error.classify() {
        serde_json::error::Category::Syntax => "it is not valid JSON",
        serde_json::error::Category::Eof => "it ends before one complete JSON object",
        serde_json::error::Category::Data => {
            "a field is missing, unknown, duplicated or of the wrong type"
        }
        serde_json::error::Category::Io => "it could not be read",
    };
    format!(
        "{kind} (line {}, column {}); escape every control character inside strings",
        error.line(),
        error.column()
    )
}

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
    prefix: Vec<ModelMessage>,
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

/// The pre-stage history through `through`, which the summary covers and the
/// receipt's cut names, and the leading system messages of the request the
/// step sent, which only the summary request carries so it reads the
/// response's cached prefix.
fn review_source(
    base: &crate::context::Context,
    sent: Option<&[ModelMessage]>,
    through: usize,
    io: Option<&PreparationIo>,
) -> Result<(crate::context::Context, Vec<ModelMessage>, u64)> {
    let (source, bytes) = with_history_io(io, |work| {
        crate::context::clone_context(base, Some(through), work)
    })?;
    let header = sent.map_or(&[][..], |sent| {
        let count = sent
            .iter()
            .take_while(|message| message.role == crate::model::ModelRole::System)
            .count();
        &sent[..count]
    });
    let (prefix, prefix_bytes) = with_history_io(io, |work| {
        crate::context::clone_messages(header, None, &[], work)
    })?;
    let bytes = bytes.checked_add(prefix_bytes).ok_or_else(|| {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "review source bytes overflow",
        )
    })?;
    Ok((source, prefix, bytes))
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
        // A step that staged no separate projection sent the pre-stage header.
        let projection = match response
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                ExecutionEvent::ContextPrepared {
                    step: recorded,
                    projection,
                    ..
                } if *recorded == step => Some(projection),
                _ => None,
            }) {
            Some(reference) => Some(self.load_response_projection(journal, reference).await?.1),
            None => None,
        };
        let (source, prefix, bytes) = review_source(
            &base,
            projection
                .as_ref()
                .map(|prepared| prepared.request().messages.as_slice()),
            through,
            self.io,
        )?;
        drop(projection);
        #[cfg(all(test, not(target_arch = "wasm32")))]
        acyclic_test_support::differential::record((
            "context review source messages",
            through as u64,
        ));
        let hold = self.io.map(|io| io.guard_owned(bytes)).transpose()?;
        let job = Job {
            step,
            source,
            prefix,
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
                    (job.source, job.prefix, job.selected),
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
        // The receipt's cut names the pre-stage history; the stages run again
        // on the compacted base below.
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
        source: (
            crate::context::Context,
            Vec<ModelMessage>,
            Option<Arc<StepBinding>>,
        ),
        step: u32,
        observation: Observation,
        phase: &mut DispatchPhase,
    ) -> Result<Option<Receipt<'a>>> {
        let (source, prefix, selected) = source;
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
                (source.messages.len(), Some(&instruction), &prefix),
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
        let material =
            session.manager_material(&source, 0..end, None, Some(&instruction), &prefix)?;
        let output = session
            .summarize_output(
                journal,
                Some(input),
                input.operation_id,
                step,
                material,
                &prefix,
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
                    .reject_decision(journal, (input.operation_id, step), defect.to_owned())
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
        // Only the reply's JSON syntax and shape are the reviewer's defect.
        // The budgeted decode below runs on a reply that already parsed, so
        // its failures (cancellation, an exhausted budget) stay the host's.
        let shape = crate::contract::json_from_slice::<Decision>(text.as_bytes());
        let (decision, decision_hold) = match (shape, self.io) {
            (Err(error), _) => {
                stock_retire_text(text, self.io)?;
                drop(summary);
                drop(hold);
                return self
                    .reject_decision(
                        journal,
                        (input.operation_id, step),
                        decode_rejection(&error),
                    )
                    .await;
            }
            (Ok(decision), None) => (decision, None),
            (Ok(parsed), Some(io)) => {
                drop(parsed);
                io.decode(
                    std::marker::PhantomData::<Decision>,
                    text.as_bytes(),
                    crate::wire::ErrorCode::Invalid,
                )
                .map(|(decision, hold)| (decision, Some(hold)))?
            }
        };
        stock_retire_text(text, self.io)?;
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
                    journal,
                    (input.operation_id, step),
                    "version must be 1, review_at_input_tokens must be positive and below the capacity, and summary must be nonempty, or null with review_at_input_tokens above the current input".to_owned(),
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
    /// the worker's: the context stays unreviewed. Each rejection is fed back
    /// at the next response boundary. Boundaries provide backpressure without
    /// imposing a retry count; the independent capacity backstop still compacts.
    async fn reject_decision(
        &self,
        journal: &dyn ExecutionJournal,
        (operation, step): (OperationId, u32),
        reason: String,
    ) -> Result<Option<Receipt<'a>>> {
        // The stream records why the settled reply authorised nothing, so a
        // reader sees the defect without the model call's private output.
        let key = with_history_io(self.io, |work| {
            preparation_format(format_args!("summary:{step}:rejected"), work)
        })?;
        let event = ExecutionEvent::ModelRejected {
            step,
            purpose: ModelPurpose::Summary,
            reason: reason.clone(),
        };
        phase::PreparedPhase::new(journal, self.io)?
            .run(async {
                let _writer = self.journal_writes.lock().await;
                journal.append(operation, &key, &event, self.io)?.await
            })
            .await?;
        self.review.state.lock().await.rejected = Some(reason);
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
                (job.source, job.prefix, job.selected),
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
            acyclic_fs_work::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024),
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
                prefix: Vec::new(),
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

    /// The summary window is chosen with the header its request carries. A
    /// history that fits alone but not under the header skips the review
    /// before any paid admission, instead of failing the worker.
    #[tokio::test]
    async fn a_review_that_fits_only_without_its_header_is_skipped() -> Result<()> {
        use crate::model::{ModelMessage, ModelRole};
        let provider = Arc::new(
            crate::test_support::ScriptedModelProvider::new([
                crate::test_support::ScriptedTurn::Text(
                    serde_json::json!({"version": 1, "summary": null, "review_at_input_tokens": 8_000})
                        .to_string(),
                ),
            ])
            .with_context_capacity(crate::context::ModelContextCapacity {
                context_tokens: 8192,
                output_tokens: Some(512),
                output_shares_context: false,
            })
            .with_token_counter(|request| {
                Ok(crate::context::ModelTokenCount::from_upper_bounds(
                    request.request_digest(),
                    u32::try_from(request.bytes().len())
                        .map_err(|_| Error::Invalid("fixture request exceeds u32".into()))?,
                    vec![0; request.request().messages.len()],
                ))
            }),
        );
        let stock = StockExecutor::new(
            Model::new("fixture".into(), "selected".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_automatic_compaction()?;
        let storage =
            crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
        let journal = storage.journal();
        let execution = StockExecution::ordinary(&stock);
        let source = crate::context::Context {
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("history ".repeat(400)),
            }],
            ..Default::default()
        };
        let header = vec![ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text("rules ".repeat(1_200)),
        }];
        let observation = Observation {
            tokens: ModelTokenUsage::default(),
            input_tokens: 4096,
            latency_ns: None,
        };
        let review = |prefix: Vec<ModelMessage>| {
            let input = TurnInput {
                operation_id: OperationId::new(),
                max_steps: None,
                input: ModelContent::Text("Continue.".into()),
                selected_context: None,
            };
            let source = source.clone();
            let execution = &execution;
            let journal = journal.as_ref();
            async move {
                execution
                    .evaluate_context_review(
                        journal,
                        &input,
                        (source, prefix, None),
                        0,
                        observation,
                        &mut DispatchPhase::new(),
                    )
                    .await
                    .map(|receipt| receipt.is_some())
            }
        };
        assert!(matches!(review(header).await, Ok(false)));
        assert_eq!(provider.request_count(), 0);
        // Control: the same history without the header fits and is reviewed.
        review(Vec::new()).await?;
        assert_eq!(provider.request_count(), 1);
        Ok(())
    }

    /// The summary request carries the selected header and every reviewed
    /// message, while the cut it authorizes stays on the pre-stage history.
    #[test]
    fn review_source_keeps_the_cut_pre_stage_and_every_reviewed_message() -> Result<()> {
        use crate::model::{ModelContent, ModelMessage, ModelRole};
        let message = |role, text: &str| ModelMessage {
            role,
            content: ModelContent::Text(text.into()),
        };
        let conversation = [
            message(ModelRole::User, "earlier request"),
            message(ModelRole::Assistant, "earlier answer"),
            message(ModelRole::User, "current request"),
        ];
        let header = [
            message(ModelRole::System, "prepended instructions"),
            message(ModelRole::System, "selected instructions"),
        ];
        // A replaced header, and a stage that prepends instructions to a base
        // without one.
        for base_header in [&[message(ModelRole::System, "old instructions")][..], &[]] {
            let base = crate::context::Context {
                messages: base_header.iter().chain(&conversation).cloned().collect(),
                current_input_index: Some(2 + u32::from(!base_header.is_empty())),
                ..Default::default()
            };
            let projection = crate::context::Context {
                messages: header.iter().chain(&conversation).cloned().collect(),
                current_input_index: Some(4),
                ..Default::default()
            };
            let through = base_header.len() + 2;
            let (source, prefix, _) =
                review_source(&base, Some(&projection.messages), through, None)?;
            assert_eq!(source.messages, base.messages[..through]);
            let request = crate::executor::summary::with_request_prefix(source, &prefix, None)?;
            let reviewed: Vec<_> = header.iter().chain(&conversation[..2]).cloned().collect();
            assert_eq!(request.messages, reviewed);
            // Without a staged projection the request keeps the base header.
            let (source, prefix, _) = review_source(&base, None, through, None)?;
            assert!(prefix.is_empty());
            assert_eq!(
                crate::executor::summary::with_request_prefix(source, &prefix, None)?.messages,
                base.messages[..through]
            );
        }
        Ok(())
    }
}
