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

#[derive(Clone, Copy, Debug)]
struct Observation {
    tokens: ModelTokenUsage,
    latency_ns: Option<u64>,
}

struct Job<'a> {
    step: u32,
    source: crate::context::Context,
    observation: Observation,
    _hold: Option<crate::conversation::ContentHold<'a>>,
}

struct Receipt<'a> {
    summary: crate::context::ContextSummary,
    _hold: Option<crate::conversation::ContentHold<'a>>,
}

#[derive(Default)]
struct State<'a> {
    review_at: u64,
    queued: Option<Job<'a>>,
    active: bool,
    ready: Option<Result<Option<Receipt<'a>>>>,
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
        let Some(input_tokens) = usage.input else {
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
            observation,
            _hold: hold,
        };
        if completed {
            drop(state);
            let mut phase = DispatchPhase::new();
            let ready = self
                .evaluate_context_review(journal, input, job.source, step, observation, &mut phase)
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
                capacity,
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
        source: crate::context::Context,
        step: u32,
        observation: Observation,
        phase: &mut DispatchPhase,
    ) -> Result<Option<Receipt<'a>>> {
        let capacity = self.model_capacity()?;
        let instruction = with_history_io(self.io, |work| {
            preparation_format(
                format_args!(
                    "{INSTRUCTION}\nSelected model: {}\nCapacity: {}\nOriginal worker observation: {observation:?}\nPrices not supplied are unknown; do not invent prices.",
                    self.stock.model.name, capacity.context_tokens
                ),
                work,
            )
        })?;
        let (summary, text) = self
            .summarize(
                journal,
                Some(input),
                input.operation_id,
                step,
                (source, ModelContent::Text(instruction)),
                phase,
            )
            .await?;
        let hold = self
            .io
            .map(|io| io.guard_owned(summary.output.logical_owned_bytes()?))
            .transpose()?;
        let (decision, decision_hold): (Decision, _) = match self.io {
            Some(io) => io
                .decode(
                    std::marker::PhantomData::<Decision>,
                    text.as_bytes(),
                    crate::wire::ErrorCode::Invalid,
                )
                .map(|(decision, hold)| (decision, Some(hold)))?,
            None => (
                crate::contract::json_from_slice(text.as_bytes()).map_err(|_| {
                    stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "context review returned an invalid decision",
                    )
                })?,
                None,
            ),
        };
        stock_retire_text(text, self.io)?;
        let valid = decision.version == 1
            && decision.review_at_input_tokens > 0
            && decision.review_at_input_tokens < u64::from(capacity.context_tokens)
            && match &decision.summary {
                Some(text) => !text.trim().is_empty(),
                None => decision.review_at_input_tokens > observation.tokens.input.unwrap_or(0),
            };
        let (rewrite, next) = (decision.summary.is_some(), decision.review_at_input_tokens);
        drop(decision);
        drop(decision_hold);
        if !valid {
            drop(summary);
            drop(hold);
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "context review must supply a nonempty summary or a future input budget",
            ));
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
            _hold: hold,
        }))
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
                job.source,
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
            if result.is_err() {
                self.stop_context_review(journal, input.operation_id, step)
                    .await?;
            }
            let mut state = self.review.state.lock().await;
            state.active = false;
            state.ready = Some(result);
            drop(state);
            self.review.completed.notify_one();
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
