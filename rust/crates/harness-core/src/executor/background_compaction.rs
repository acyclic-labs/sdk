//! Speculative context work scoped to the execution that owns its journal.
//! There is one source cut, never a queue proportional to history or steps.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify};

pub(super) const INSTRUCTION: &str = "Summarize the preceding context for the continuing worker. Preserve the task, decisions, constraints, unfinished work, exact identifiers and essential tool results. Return only the working summary, without performing the task.";

#[derive(Clone, Copy)]
struct Cut {
    step: u32,
    messages: usize,
    digest: [u8; 32],
}

struct Job<'a> {
    source: crate::context::Context,
    _hold: Option<crate::conversation::ContentHold<'a>>,
}

struct Receipt<'a> {
    summary: crate::context::ContextSummary,
    hold: Option<crate::conversation::ContentHold<'a>>,
}

#[derive(Default)]
struct State<'a> {
    cut: Option<Cut>,
    queued: Option<Job<'a>>,
    ready: Option<Result<Receipt<'a>>>,
}

#[derive(Default)]
pub(in crate::executor) struct Background<'a> {
    enabled: AtomicBool,
    stopped: AtomicBool,
    worker_paused: AtomicBool,
    state: Mutex<State<'a>>,
    requested: Notify,
    completed: Notify,
    stop: Notify,
}

impl StockExecution<'_> {
    pub(super) async fn restore_background(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior: &[ModelMessage],
        recorded: &crate::model::PreparedModelRequest,
    ) -> Result<()> {
        if !self.background_available().await {
            return Ok(());
        }
        // The original summary admission, including a stop or partial stream,
        // is authoritative on recovery. Never replace a failed context call
        // with a mechanical eviction just because its in-memory job was lost.
        let (_, summaries) = self
            .model_records(journal, input.operation_id, step, ModelPurpose::Summary)
            .await?;
        if summaries.iter().any(|record| {
            matches!(
                record.event,
                ExecutionEvent::ModelStopped {
                    worker_paused: true,
                    ..
                }
            )
        }) {
            // A yield retires only speculation. Resume the worker normally;
            // its later steps can prepare a fresh cut, and the capacity kernel
            // still protects the next request if pressure arrives first.
            return Ok(());
        }
        if summaries
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ModelStarted { .. }))
        {
            let restored = self
                .prepared_response(journal, input, step, prior, None)
                .await?;
            if restored.request_digest() != recorded.request_digest() {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Storage,
                    "background recovery changed the admitted worker request",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn background_available(&self) -> bool {
        self.background.enabled.load(Ordering::Acquire)
            && self.background.state.lock().await.cut.is_none()
    }

    /// The worker owns shutdown; an unused speculative call never delays its
    /// terminal reply. Cancellation is journalled and its accepted writes sealed.
    pub(super) async fn with_background<T>(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        worker: impl std::future::Future<Output = Result<ExecutionProgress<T>>>,
    ) -> Result<ExecutionProgress<T>> {
        self.background.stopped.store(false, Ordering::Release);
        self.background.enabled.store(true, Ordering::Release);
        let worker = async {
            let result = worker.await;
            self.background.worker_paused.store(
                matches!(result, Ok(ExecutionProgress::Pending(_))),
                Ordering::Release,
            );
            self.background.stopped.store(true, Ordering::Release);
            self.background.stop.notify_one();
            result
        };
        let (result, cleanup) = futures::join!(worker, self.background_service(journal, input));
        self.background.enabled.store(false, Ordering::Release);
        // Retire any unused output and its custody before the caller's IO dies.
        *self.background.state.lock().await = State::default();
        cleanup?;
        result
    }

    pub(super) async fn queue_background(
        &self,
        context: &crate::context::Context,
        through: usize,
        step: u32,
    ) -> Result<()> {
        if !self.background.enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut state = self.background.state.lock().await;
        if state.cut.is_some() {
            return Ok(());
        }
        let digest = crate::context::prefix_digest(context, through, self.io)?;
        let (source, bytes) = with_history_io(self.io, |work| {
            crate::context::clone_context(context, Some(through), work)
        })?;
        let hold = self.io.map(|io| io.guard_owned(bytes)).transpose()?;
        state.cut = Some(Cut {
            step,
            messages: through,
            digest,
        });
        state.queued = Some(Job {
            source,
            _hold: hold,
        });
        drop(state);
        self.background.requested.notify_one();
        Ok(())
    }

    fn background_cut_matches(&self, cut: Cut, context: &crate::context::Context) -> Result<bool> {
        Ok(cut.messages < context.messages.len()
            && crate::context::prefix_digest(context, cut.messages, self.io)? == cut.digest)
    }

    pub(super) async fn background_source_matches(
        &self,
        context: &crate::context::Context,
    ) -> Result<bool> {
        let cut = self.background.state.lock().await.cut;
        cut.map_or(Ok(false), |cut| self.background_cut_matches(cut, context))
    }

    /// Only a matching immutable prefix can be adopted. Waiting is backpressure
    /// at the actual capacity boundary, never a timer or a readiness probe.
    pub(super) async fn background_summary(
        &self,
        context: &crate::context::Context,
    ) -> Result<Option<crate::context::ContextSummary>> {
        loop {
            let completed = self.background.completed.notified();
            let mut state = self.background.state.lock().await;
            let Some(cut) = state.cut else {
                return Ok(None);
            };
            if !self.background_cut_matches(cut, context)? {
                // A stage changed the source. Keep its call's publication owner,
                // but do not use that receipt to rewrite another prefix.
                return Ok(None);
            }
            if let Some(result) = state.ready.take() {
                state.cut = None;
                let receipt = result?;
                if let Some(hold) = receipt.hold {
                    hold.into_retained()?;
                }
                return Ok(Some(receipt.summary));
            }
            drop(state);
            completed.await;
        }
    }

    async fn background_service(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<()> {
        loop {
            let requested = self.background.requested.notified();
            let stopped = self.background.stop.notified();
            if self.background.stopped.load(Ordering::Acquire) {
                return Ok(());
            }
            let job = {
                let mut state = self.background.state.lock().await;
                state.queued.take().zip(state.cut)
            };
            let Some((job, cut)) = job else {
                futures::pin_mut!(requested, stopped);
                let _ = futures::future::select(requested, stopped).await;
                continue;
            };
            let mut phase = DispatchPhase::new();
            let summary = async {
                let instruction = ModelContent::Text(stock_string_copy(INSTRUCTION, self.io)?);
                let (summary, text) = self
                    .summarize(
                        journal,
                        Some(input),
                        input.operation_id,
                        cut.step,
                        (job.source, instruction),
                        &mut phase,
                    )
                    .await?;
                stock_retire_text(text, self.io)?;
                let hold = self
                    .io
                    .map(|io| io.guard_owned(summary.output.logical_owned_bytes()?))
                    .transpose()?;
                Ok(Receipt { summary, hold })
            };
            let result = {
                futures::pin_mut!(summary, stopped);
                match futures::future::select(summary, stopped).await {
                    futures::future::Either::Left((result, _)) => Some(result),
                    futures::future::Either::Right(_) => None,
                }
            };
            drop(job._hold);
            let Some(result) = result else {
                return self
                    .stop_background_call(
                        journal,
                        input.operation_id,
                        cut.step,
                        self.background.worker_paused.load(Ordering::Acquire),
                    )
                    .await;
            };
            let result = match result {
                Err(error) => match self
                    .stop_background_call(journal, input.operation_id, cut.step, false)
                    .await
                {
                    Ok(()) => Err(error),
                    Err(error) => Err(error),
                },
                result => result,
            };
            self.background.state.lock().await.ready = Some(result);
            self.background.completed.notify_one();
        }
    }

    async fn stop_background_call(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
        worker_paused: bool,
    ) -> Result<()> {
        // The provider future is gone before recording producer cancellation.
        // A completed call can have unused output; it is never relabelled stopped.
        let completion = phase::PreparedPhase::new(journal, self.io)?;
        completion
            .run(async {
                let _writer = self.journal_writes.lock().await;
                let (tail, records) = self
                    .model_records(journal, operation, step, ModelPurpose::Summary)
                    .await?;
                if records
                    .iter()
                    .any(|record| matches!(record.event, ExecutionEvent::ModelStarted { .. }))
                    && !records.iter().any(|record| {
                        matches!(
                            record.event,
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
                                worker_paused,
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
