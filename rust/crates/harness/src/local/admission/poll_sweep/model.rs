//! Scripted effects retain receipts independently of the cancelled client future.
use super::{schedule::Schedule, *};
use crate::{
    context::{ModelContextCapacity, ModelTokenCount},
    conversation::FileRef,
    model::{ModelDispatch, ModelEvent, PreparedModelRequest},
    runtime::ToolContext,
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use futures::StreamExt;
use std::{collections::VecDeque, sync::Mutex};

pub(super) struct ModelEffects {
    accounting: ScriptedModelProvider,
    turns: Mutex<VecDeque<ScriptedTurn>>,
    pub schedule: Arc<Schedule>,
    // Single executor; never held across an await, no nested locks.
    responses: Mutex<Vec<(ModelDispatch, Vec<ModelEvent>)>>,
    summary_files: Mutex<Vec<FileRef>>,
    // Real providers retain no receipt a reopened host can reconcile.
    forgotten: Mutex<bool>,
}

impl ModelEffects {
    pub fn summary_files(&self) -> Vec<FileRef> {
        self.summary_files.lock().unwrap().clone()
    }

    pub fn dispatches(&self) -> Vec<ModelDispatch> {
        self.responses
            .lock()
            .unwrap()
            .iter()
            .map(|(dispatch, _)| *dispatch)
            .collect()
    }
    pub fn forget(&self, turns: Vec<ScriptedTurn>) {
        *self.forgotten.lock().unwrap() = true;
        *self.turns.lock().unwrap() = turns.into();
    }
    pub fn accepted(&self) -> usize {
        self.responses.lock().unwrap().len()
    }

    pub fn new(turns: Vec<ScriptedTurn>, schedule: Arc<Schedule>, compact: bool) -> Self {
        let mut script = ScriptedModelProvider::new([]);
        if compact {
            script = script
                .with_context_capacity(ModelContextCapacity {
                    context_tokens: 1024,
                    output_tokens: Some(64),
                })
                .with_token_counter(|request| {
                    Ok(ModelTokenCount::from_upper_bounds(
                        request.request_digest(),
                        10,
                        request
                            .request()
                            .messages
                            .iter()
                            .map(|m| match &m.content {
                                _ if m
                                    .content
                                    .file_refs()
                                    .any(|file| file.path() == "history/0.txt") =>
                                {
                                    800
                                }
                                _ if m
                                    .content
                                    .file_refs()
                                    .any(|file| file.path().starts_with("history/")) =>
                                {
                                    100
                                }
                                _ => 10,
                            })
                            .collect(),
                    ))
                });
        }
        Self {
            accounting: script,
            turns: Mutex::new(turns.into()),
            schedule,
            responses: Mutex::default(),
            summary_files: Mutex::default(),
            forgotten: Mutex::default(),
        }
    }
}

impl ModelProvider for ModelEffects {
    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        self.accounting.context_capacity(model)
    }
    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> BoxProviderFuture<'a, Result<ModelTokenCount>> {
        let mut summaries = self.summary_files.lock().unwrap();
        for file in request
            .request()
            .messages
            .iter()
            .flat_map(|m| m.content.file_refs())
        {
            if file.path().starts_with("summaries/") && !summaries.contains(file) {
                summaries.push(file.clone());
            }
        }
        drop(summaries);
        self.accounting.count_tokens(request)
    }

    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, Result<ModelEvent>> {
        Box::pin(
            futures::stream::once(async move {
                self.schedule.describe(|| format!("model {dispatch:?}"));
                self.schedule.point("model:dispatch").await;
                let active = self.schedule.dispatch("model");
                let turn = self
                    .turns
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("script exhausted");
                let script = ScriptedModelProvider::new([turn]);
                let events: Vec<_> = script.generate(request, dispatch).collect().await;
                let events: Result<Vec<_>> = events.into_iter().collect();
                let events = events?;
                assert!(
                    !self
                        .responses
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|(d, _)| *d == dispatch),
                    "duplicate model dispatch"
                );
                self.responses
                    .lock()
                    .unwrap()
                    .push((dispatch, events.clone()));
                drop(active);
                self.schedule.point("model:accepted").await;
                Ok::<_, Error>(events)
            })
            .map(|events| {
                futures::stream::iter(match events {
                    Ok(events) => events.into_iter().map(Ok).collect(),
                    Err(error) => vec![Err(error)],
                })
            })
            .flatten(),
        )
    }

    fn reconcile<'a>(
        &'a self,
        dispatch: ModelDispatch,
        observed: &'a [ModelEvent],
    ) -> BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            self.schedule.point("model:reconcile").await;
            if *self.forgotten.lock().unwrap() {
                return Ok(None);
            }
            let events = self
                .responses
                .lock()
                .unwrap()
                .iter()
                .find(|(d, _)| *d == dispatch)
                .map(|(_, e)| e.clone());
            let events = match events {
                Some(events) => events,
                None => {
                    // The external server finishes the original accepted reservation.
                    // This is distinct from a new execute/generate attempt.
                    let active = self.schedule.dispatch("model:finish-reservation");
                    let turn = self
                        .turns
                        .lock()
                        .unwrap()
                        .pop_front()
                        .expect("script exhausted");
                    let events = turn
                        .stream()
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>>>()?;
                    self.responses
                        .lock()
                        .unwrap()
                        .push((dispatch, events.clone()));
                    drop(active);
                    events
                }
            };
            assert!(
                events.starts_with(observed),
                "changed model reservation or prefix"
            );
            // The shared scripted provider validates the exact reconciliation prefix.
            ScriptedModelProvider::new([])
                .with_reconciliations([ScriptedReconciliation {
                    orphan: true,
                    observed: observed.to_vec(),
                    continuation: Some(events[observed.len()..].to_vec()),
                }])
                .reconcile(dispatch, observed)
                .await
        })
    }
}

pub(super) fn tool_turn() -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
            "effect".into(),
            "test.effect".into(),
            serde_json::json!({}),
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: serde_json::Value::Null,
        }),
    ])
}

pub(super) struct Effect {
    pub schedule: Arc<Schedule>,
    // A non-idempotent external effect: execute twice increments twice and fails.
    pub calls: Mutex<Vec<ToolInvocation>>,
    pub forgotten: Mutex<bool>,
}
impl ToolExecutor for Effect {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.schedule.describe(|| {
                format!(
                    "tool {} {} {}",
                    invocation.operation_id, invocation.name, invocation.call_id
                )
            });
            self.schedule.point("tool:dispatch").await;
            let active = self.schedule.dispatch("tool");
            self.calls.lock().unwrap().push(invocation);
            assert_eq!(
                self.calls.lock().unwrap().len(),
                1,
                "duplicated external effect"
            );
            drop(active);
            self.schedule.point("tool:accepted").await;
            Ok(ToolResult {
                value: serde_json::json!({"receipt":1}),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.schedule.point("tool:reconcile").await;
            if *self.forgotten.lock().unwrap() {
                return Ok(None);
            }
            if self.calls.lock().unwrap().is_empty() {
                let active = self.schedule.dispatch("tool:finish-reservation");
                self.calls.lock().unwrap().push(invocation.clone());
                drop(active);
            }
            Ok(self
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|call| {
                    call.operation_id == invocation.operation_id
                        && call.call_id == invocation.call_id
                })
                .then(|| ToolResult {
                    value: serde_json::json!({"receipt":1}),
                }))
        })
    }
}
