//! Model-owned rewrite decisions, driven alongside the worker without a task.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify};

const INSTRUCTION: &str = "You manage this worker's context outside its turns. Decide using the supplied original usage and the work still needed, including the cost of another summary, cache rebuild and repeated cached input, and the latency of attending to a growing history. Do not wait for the context window to fill. Preserve instructions, facts, decisions, constraints, evidence references and unfinished work. Return only JSON with exactly two fields: summary (a concise replacement for the supplied history, or null when keeping it is more economical) and review_at_input_tokens (a positive input-token count at which to reconsider). This is your budget, not the model capacity. When keeping history, choose a count greater than the current reported input. When summarising, choose the next review count for the smaller replacement context. Never remove facts required by ongoing work.";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Decision {
    summary: Option<String>,
    review_at_input_tokens: u64,
}

struct Job<'a> {
    step: u32,
    source: crate::context::Context,
    usage: ModelTokenUsage,
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
    requested: Notify,
    pub(super) completed: Notify,
    stop: Notify,
}

impl StockExecution<'_> {
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
        events: &[ModelEvent],
    ) -> Result<()> {
        if !self.review.enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(usage) = events.iter().rev().find_map(|event| match event {
            ModelEvent::Completed { metadata } => Some(ModelTokenUsage::reported(metadata)),
            _ => None,
        }) else {
            return Ok(());
        };
        let Some(input_tokens) = usage.input else {
            return Ok(());
        };
        let mut state = self.review.state.lock().await;
        if state.active || state.ready.is_some() || input_tokens < state.review_at {
            return Ok(());
        }
        let crate::context::CompactionPolicy::Capacity(policy) = self.compaction_policy()? else {
            return Ok(());
        };
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
        let hold = self.io.map(|io| io.guard_owned(bytes)).transpose()?;
        state.queued = Some(Job {
            step,
            source,
            usage,
            _hold: hold,
        });
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

    async fn context_review_service(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<()> {
        loop {
            let requested = self.review.requested.notified();
            let stopped = self.review.stop.notified();
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
            let review = async {
                let instruction = with_history_io(self.io, |work| {
                    preparation_format(
                        format_args!(
                            "{INSTRUCTION}\nSelected model: {}\nOriginal worker usage: {}",
                            self.stock.model.name,
                            serde_json::to_string(&job.usage)?
                        ),
                        work,
                    )
                })?;
                let (original, text) = self
                    .summarize(
                        journal,
                        Some(input),
                        input.operation_id,
                        step,
                        (job.source, ModelContent::Text(instruction)),
                        &mut phase,
                    )
                    .await?;
                let decision: Decision = crate::contract::json_from_slice(text.as_bytes())?;
                stock_retire_text(text, self.io)?;
                if decision.review_at_input_tokens == 0
                    || (decision.summary.is_none()
                        && decision.review_at_input_tokens <= job.usage.input.unwrap_or(0))
                {
                    return Err(stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "context review must select a future input budget",
                    ));
                }
                self.review.state.lock().await.review_at = decision.review_at_input_tokens;
                let Some(text) = decision.summary else {
                    return Ok(None);
                };
                if text.trim().is_empty() {
                    return Err(stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "context review returned an empty replacement",
                    ));
                }
                let key = with_history_io(self.io, |work| {
                    preparation_format(format_args!("review:{step}:summary"), work)
                })?;
                let summary = self
                    .publish_summary(
                        journal,
                        (input.operation_id, step),
                        (original.source_messages, original.source_digest),
                        (&key, text.as_bytes()),
                    )
                    .await?;
                let hold = self
                    .io
                    .map(|io| io.guard_owned(summary.output.logical_owned_bytes()?))
                    .transpose()?;
                Ok(Some(Receipt {
                    summary,
                    _hold: hold,
                }))
            };
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
