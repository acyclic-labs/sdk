//! Fully replaceable turn execution and the stock streaming model/tool loop.

use crate::{
    Error, InteractionId, OperationId, Result,
    context::{ContextInput, ContextPipeline},
    interaction::{Interaction, InteractionResponse},
    model::{Model, ModelAttempt, ModelEvent, ModelMessage, ModelProvider, ModelRequest},
    tool::{ToolInvocation, ToolRegistry, ToolResult, validate_value},
};
use futures::{StreamExt as _, future::BoxFuture};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

/// Durable input to any custom executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnInput {
    /// Stable durable turn execution identity.
    pub operation_id: OperationId,
    /// Application-defined input.
    pub input: Value,
    /// Maximum model/tool steps permitted for this turn.
    pub max_steps: u32,
}

/// Gapless replay record returned by a durable execution journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRecord {
    /// Owning turn execution.
    pub operation_id: OperationId,
    /// Gapless one-based journal sequence.
    pub sequence: u64,
    /// Stable per-step retry identity.
    pub idempotency_key: String,
    /// Canonical observation.
    pub event: ExecutionEvent,
}

/// Canonical executor observation suitable for a durable journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecutionEvent {
    /// Binds an operation identity to one immutable request and composition.
    Started {
        /// Digest of the input, stock executor version, model, stages, and tools.
        request_digest: [u8; 32],
    },
    /// A model request identity committed before provider dispatch.
    ModelStarted {
        /// Zero-based executor step.
        step: u32,
        /// Digest of the exact model request.
        request_digest: [u8; 32],
        /// Complete model-visible request retained for audit and exact recovery.
        request: ModelRequest,
    },
    /// One model stream item was observed.
    Model {
        /// Zero-based executor step.
        step: u32,
        /// Observed model event.
        event: ModelEvent,
    },
    /// Tool dispatch is about to begin.
    ToolStarted {
        /// Zero-based executor step.
        step: u32,
        /// Admitted invocation.
        invocation: ToolInvocation,
    },
    /// Tool execution and projection completed.
    ToolCompleted {
        /// Zero-based executor step.
        step: u32,
        /// Completed invocation.
        invocation: ToolInvocation,
        /// Validated executor result.
        result: ToolResult,
        /// Model-visible projection.
        projection: Value,
    },
}

/// Durable host services available to an executor; policy remains executor-owned.
pub trait ExecutionJournal: Send + Sync {
    /// Implementations must scope sequences and retry keys by `operation_id`.
    /// Replays the complete retained journal before execution resumes.
    fn replay<'a>(
        &'a self,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>>;

    /// Appends one reconstructable executor observation.
    fn append<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<()>>;

    /// Opens one typed durable interaction exactly once.
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<()>>;

    /// Reads a validated durable response without blocking the semantic core.
    fn interaction_response<'a>(
        &'a self,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<InteractionResponse>>>;
}

/// Terminal result produced by an executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnOutput {
    /// User-visible assistant text.
    pub text: String,
    /// Provider-owned final metadata.
    pub metadata: Value,
    /// Number of completed model steps.
    pub steps: u32,
}

/// Complete replaceable turn loop. Implementations may own every policy decision.
pub trait Executor: Send + Sync {
    /// Executes or resumes one turn using only explicit durable host services.
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>>;
}

/// Complete default streaming model/tool loop assembled from replaceable values.
#[derive(Clone)]
pub struct StockExecutor {
    model: Model,
    provider: Arc<dyn ModelProvider>,
    context: ContextPipeline,
    tools: ToolRegistry,
}

impl StockExecutor {
    /// Creates the stock loop without installing hidden stages or tools.
    #[must_use]
    pub fn new(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
        tools: ToolRegistry,
    ) -> Self {
        Self {
            model,
            provider,
            context,
            tools,
        }
    }

    fn request_digest(&self, input: &TurnInput) -> Result<[u8; 32]> {
        let canonical = serde_json::to_vec(&json!({
            "executor": "acyclic.stock.v1",
            "input": input,
            "model": self.model,
            "context": self.context.contracts(),
            "tools": self.tools.definitions(),
        }))
        .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(*blake3::hash(&canonical).as_bytes())
    }
}

impl Executor for StockExecutor {
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        Box::pin(async move {
            if input.max_steps == 0 {
                return Err(Error::Invalid("max_steps must be positive".into()));
            }
            let records = journal.replay(input.operation_id).await?;
            for (index, record) in records.iter().enumerate() {
                if record.operation_id != input.operation_id || record.sequence != index as u64 + 1
                {
                    return Err(Error::Conflict(
                        "execution journal is not gapless or belongs to another turn".into(),
                    ));
                }
            }
            let request_digest = self.request_digest(&input)?;
            match records.first().map(|record| &record.event) {
                Some(ExecutionEvent::Started {
                    request_digest: existing,
                }) if existing == &request_digest => {}
                Some(_) => {
                    return Err(Error::Conflict(
                        "execution identity is bound to another request or configuration".into(),
                    ));
                }
                None => {
                    journal
                        .append(
                            input.operation_id,
                            "execution:started".into(),
                            ExecutionEvent::Started { request_digest },
                        )
                        .await?;
                }
            }
            let mut prior_messages = Vec::new();
            let mut text = String::new();
            for step in 0..input.max_steps {
                let context = self
                    .context
                    .run(&ContextInput {
                        input: input.input.clone(),
                        step,
                        prior_messages: prior_messages.clone(),
                    })
                    .await?;
                let mut calls = Vec::new();
                let mut completed = None;
                let replayed_model = records
                    .iter()
                    .filter_map(|record| match &record.event {
                        ExecutionEvent::Model {
                            step: event_step,
                            event,
                        } if *event_step == step => Some(event.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let request = ModelRequest {
                    model: self.model.clone(),
                    messages: context.messages,
                    tools: self.tools.definitions(),
                    max_output_tokens: None,
                };
                let request_digest = model_request_digest(&request)?;
                let started = records.iter().find_map(|record| match &record.event {
                    ExecutionEvent::ModelStarted {
                        step: event_step,
                        request_digest,
                        request,
                    } if *event_step == step => Some((*request_digest, request)),
                    _ => None,
                });
                if started.is_some_and(|(existing_digest, existing_request)| {
                    existing_digest != request_digest || existing_request != &request
                }) {
                    return Err(Error::Conflict(
                        "model attempt identity is bound to another request".into(),
                    ));
                }
                let replay_completed = replayed_model
                    .iter()
                    .any(|event| matches!(event, ModelEvent::Completed { .. }));
                let model_events = if replay_completed {
                    replayed_model
                } else if started.is_some() {
                    let Some(mut continuation) = self
                        .provider
                        .reconcile(ModelAttempt {
                            operation_id: input.operation_id,
                            step,
                            request_digest,
                            observed: replayed_model.clone(),
                        })
                        .await?
                    else {
                        return Err(Error::Indeterminate(input.operation_id));
                    };
                    let mut observed = replayed_model;
                    for event in continuation.drain(..) {
                        journal
                            .append(
                                input.operation_id,
                                format!("model:{step}:{}", observed.len()),
                                ExecutionEvent::Model {
                                    step,
                                    event: event.clone(),
                                },
                            )
                            .await?;
                        observed.push(event);
                    }
                    observed
                } else {
                    journal
                        .append(
                            input.operation_id,
                            format!("model:{step}:started"),
                            ExecutionEvent::ModelStarted {
                                step,
                                request_digest,
                                request: request.clone(),
                            },
                        )
                        .await?;
                    let mut stream = self.provider.generate(request);
                    let mut observed = Vec::new();
                    while let Some(event) = stream.next().await {
                        let event = event?;
                        journal
                            .append(
                                input.operation_id,
                                format!("model:{step}:{}", observed.len()),
                                ExecutionEvent::Model {
                                    step,
                                    event: event.clone(),
                                },
                            )
                            .await?;
                        observed.push(event);
                    }
                    observed
                };
                validate_model_events(&model_events)?;
                for event in model_events {
                    match event {
                        ModelEvent::Content { delta } => text.push_str(&delta),
                        ModelEvent::ToolCall {
                            call_id,
                            name,
                            arguments,
                        } => {
                            calls.push(ToolInvocation {
                                call_id,
                                name,
                                arguments,
                            });
                        }
                        ModelEvent::Completed { metadata } => completed = Some(metadata),
                        ModelEvent::Reasoning { .. } => {}
                    }
                }
                let Some(metadata) = completed else {
                    return Err(Error::Indeterminate(input.operation_id));
                };
                if calls.is_empty() {
                    return Ok(TurnOutput {
                        text,
                        metadata,
                        steps: step + 1,
                    });
                }
                prior_messages.push(ModelMessage {
                    role: "assistant".into(),
                    content: json!({"tool_calls": &calls}),
                });
                for invocation in calls {
                    let tool = self
                        .tools
                        .get(&invocation.name)
                        .ok_or_else(|| Error::NotFound(format!("tool {}", invocation.name)))?;
                    validate_value(
                        &tool.definition.input_schema,
                        &invocation.arguments,
                        "tool input",
                    )?;
                    let completed_tool = records.iter().find_map(|record| match &record.event {
                        ExecutionEvent::ToolCompleted {
                            step: event_step,
                            invocation: existing,
                            result,
                            projection,
                        } if *event_step == step && existing.call_id == invocation.call_id => {
                            Some((result.clone(), projection.clone()))
                        }
                        _ => None,
                    });
                    let (result, projection) = if let Some(completed) = completed_tool {
                        completed
                    } else {
                        let started = records.iter().find_map(|record| match &record.event {
                            ExecutionEvent::ToolStarted {
                                step: event_step,
                                invocation: existing,
                            } if *event_step == step && existing.call_id == invocation.call_id => {
                                Some(existing)
                            }
                            _ => None,
                        });
                        if let Some(existing) = started {
                            if existing != &invocation {
                                return Err(Error::Conflict(
                                    "tool call identity is bound to another invocation".into(),
                                ));
                            }
                            let Some(result) = tool.executor.reconcile(invocation.clone()).await?
                            else {
                                return Err(Error::Indeterminate(input.operation_id));
                            };
                            validate_value(
                                &tool.definition.output_schema,
                                &result.value,
                                "tool output",
                            )?;
                            let projection = tool.projection.project(&invocation, &result)?;
                            journal
                                .append(
                                    input.operation_id,
                                    format!("tool:{step}:{}:completed", invocation.call_id),
                                    ExecutionEvent::ToolCompleted {
                                        step,
                                        invocation: invocation.clone(),
                                        result: result.clone(),
                                        projection: projection.clone(),
                                    },
                                )
                                .await?;
                            prior_messages.push(ModelMessage {
                                role: "tool".into(),
                                content: json!({"call_id": invocation.call_id, "name": invocation.name, "result": projection}),
                            });
                            continue;
                        }
                        journal
                            .append(
                                input.operation_id,
                                format!("tool:{step}:{}:started", invocation.call_id),
                                ExecutionEvent::ToolStarted {
                                    step,
                                    invocation: invocation.clone(),
                                },
                            )
                            .await?;
                        let result = tool.executor.execute(invocation.clone()).await?;
                        validate_value(
                            &tool.definition.output_schema,
                            &result.value,
                            "tool output",
                        )?;
                        let projection = tool.projection.project(&invocation, &result)?;
                        journal
                            .append(
                                input.operation_id,
                                format!("tool:{step}:{}:completed", invocation.call_id),
                                ExecutionEvent::ToolCompleted {
                                    step,
                                    invocation: invocation.clone(),
                                    result: result.clone(),
                                    projection: projection.clone(),
                                },
                            )
                            .await?;
                        (result, projection)
                    };
                    validate_value(&tool.definition.output_schema, &result.value, "tool output")?;
                    prior_messages.push(ModelMessage {
                        role: "tool".into(),
                        content: json!({"call_id": invocation.call_id, "name": invocation.name, "result": projection}),
                    });
                }
            }
            Err(Error::Conflict("executor step limit reached".into()))
        })
    }
}

fn model_request_digest(request: &ModelRequest) -> Result<[u8; 32]> {
    let canonical =
        serde_json::to_vec(request).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(*blake3::hash(&canonical).as_bytes())
}

fn validate_model_events(events: &[ModelEvent]) -> Result<()> {
    let mut calls = std::collections::BTreeSet::new();
    let mut completed = false;
    for (index, event) in events.iter().enumerate() {
        if completed {
            return Err(Error::Invalid(
                "model emitted an event after completion".into(),
            ));
        }
        match event {
            ModelEvent::ToolCall { call_id, name, .. } => {
                if call_id.trim().is_empty()
                    || name.trim().is_empty()
                    || !calls.insert(call_id.as_str())
                {
                    return Err(Error::Invalid(
                        "model tool calls require unique non-empty identities and names".into(),
                    ));
                }
            }
            ModelEvent::Completed { .. } => {
                completed = true;
                if index + 1 != events.len() {
                    return Err(Error::Invalid(
                        "model completion must be the final event".into(),
                    ));
                }
            }
            ModelEvent::Content { .. } | ModelEvent::Reasoning { .. } => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt as _, stream};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    struct FakeModel {
        calls: AtomicUsize,
        requests: Mutex<Vec<ModelRequest>>,
    }

    impl ModelProvider for FakeModel {
        fn generate<'a>(
            &'a self,
            request: ModelRequest,
        ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut requests) = self.requests.lock() {
                requests.push(request);
            }
            let events = if call == 0 {
                vec![
                    Ok(ModelEvent::ToolCall {
                        call_id: "call-1".into(),
                        name: "example.echo".into(),
                        arguments: json!({"value": "hello"}),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]
            } else {
                vec![
                    Ok(ModelEvent::Content {
                        delta: "done".into(),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: json!({"finish": "stop"}),
                    }),
                ]
            };
            Box::pin(stream::iter(events))
        }

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            async { Ok(None) }.boxed()
        }
    }

    struct RecoverableModel {
        generate_calls: AtomicUsize,
        reconcile_calls: AtomicUsize,
    }

    impl ModelProvider for RecoverableModel {
        fn generate<'a>(
            &'a self,
            _: ModelRequest,
        ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
            self.generate_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(stream::iter(vec![
                Ok(ModelEvent::Content {
                    delta: "partial-".into(),
                }),
                Err(Error::Storage("stream interrupted".into())),
            ]))
        }

        fn reconcile<'a>(
            &'a self,
            attempt: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt.observed
                    != vec![ModelEvent::Content {
                        delta: "partial-".into(),
                    }]
                {
                    return Err(Error::Conflict("unexpected model event prefix".into()));
                }
                Ok(Some(vec![
                    ModelEvent::Content {
                        delta: "restored".into(),
                    },
                    ModelEvent::Completed {
                        metadata: json!({"finish": "stop"}),
                    },
                ]))
            }
            .boxed()
        }
    }

    struct FakeTool(AtomicUsize);

    impl crate::tool::ToolExecutor for FakeTool {
        fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(ToolResult {
                    value: invocation.arguments,
                })
            }
            .boxed()
        }

        fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            async { Ok(None) }.boxed()
        }
    }

    struct Projection;

    impl crate::tool::ToolProjection for Projection {
        fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
            Ok(result.value.clone())
        }
    }

    #[derive(Default)]
    struct Journal(Mutex<Vec<ExecutionRecord>>);

    impl ExecutionJournal for Journal {
        fn replay<'a>(
            &'a self,
            operation_id: OperationId,
        ) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>> {
            async move {
                self.0
                    .lock()
                    .map(|records| {
                        records
                            .iter()
                            .filter(|record| record.operation_id == operation_id)
                            .cloned()
                            .collect()
                    })
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))
            }
            .boxed()
        }

        fn append<'a>(
            &'a self,
            operation_id: OperationId,
            idempotency_key: String,
            event: ExecutionEvent,
        ) -> BoxFuture<'a, Result<()>> {
            async move {
                let mut records = self
                    .0
                    .lock()
                    .map_err(|_| Error::Storage("journal lock poisoned".into()))?;
                if let Some(existing) = records.iter().find(|record| {
                    record.operation_id == operation_id && record.idempotency_key == idempotency_key
                }) {
                    return if existing.event == event {
                        Ok(())
                    } else {
                        Err(Error::Conflict("journal retry identity reused".into()))
                    };
                }
                let sequence = records
                    .iter()
                    .filter(|record| record.operation_id == operation_id)
                    .count() as u64
                    + 1;
                records.push(ExecutionRecord {
                    operation_id,
                    sequence,
                    idempotency_key,
                    event,
                });
                Ok(())
            }
            .boxed()
        }

        fn open_interaction<'a>(
            &'a self,
            _: InteractionId,
            _: Interaction,
        ) -> BoxFuture<'a, Result<()>> {
            async { Err(Error::Unsupported("interactions".into())) }.boxed()
        }

        fn interaction_response<'a>(
            &'a self,
            _: InteractionId,
        ) -> BoxFuture<'a, Result<Option<InteractionResponse>>> {
            async { Ok(None) }.boxed()
        }
    }

    #[tokio::test]
    async fn stock_loop_replays_without_reinvoking_models_or_tools() -> Result<()> {
        let model = Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let tool_executor = Arc::new(FakeTool(AtomicUsize::new(0)));
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::Tool {
            definition: crate::tool::ToolDefinition {
                name: "example.echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object"}),
            },
            executor: tool_executor.clone(),
            projection: Arc::new(Projection),
        })?;
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        );
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([1; 16]),
            input: json!("hello"),
            max_steps: 4,
        };
        let first = executor.execute(input.clone(), &journal).await?;
        let replayed = executor.execute(input, &journal).await?;
        assert_eq!(first, replayed);
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        assert_eq!(tool_executor.0.load(Ordering::SeqCst), 1);
        let requests = model
            .requests
            .lock()
            .map_err(|_| Error::Storage("model lock poisoned".into()))?;
        assert_eq!(
            requests.get(1).map(|request| request
                .messages
                .iter()
                .map(|message| message.role.as_str())
                .collect::<Vec<_>>()),
            Some(vec!["assistant", "tool"])
        );
        Ok(())
    }

    #[tokio::test]
    async fn operation_identity_rejects_changed_input() -> Result<()> {
        let model = Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let executor = StockExecutor::new(
            Model::new("example", "model", "1", Value::Null)?,
            model,
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let journal = Journal::default();
        let operation_id = OperationId::from_bytes([9; 16]);
        let _ = executor
            .execute(
                TurnInput {
                    operation_id,
                    input: json!("first"),
                    max_steps: 1,
                },
                &journal,
            )
            .await;
        assert!(matches!(
            executor
                .execute(
                    TurnInput {
                        operation_id,
                        input: json!("changed"),
                        max_steps: 1,
                    },
                    &journal,
                )
                .await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_model_stream_reconciles_without_redispatch() -> Result<()> {
        let model = Arc::new(RecoverableModel {
            generate_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
        });
        let executor = StockExecutor::new(
            Model::new("example", "recoverable", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::from_bytes([10; 16]),
            input: json!("hello"),
            max_steps: 1,
        };

        assert!(matches!(
            executor.execute(input.clone(), &journal).await,
            Err(Error::Storage(_))
        ));
        let recovered = executor.execute(input.clone(), &journal).await?;
        let replayed = executor.execute(input, &journal).await?;

        assert_eq!(recovered.text, "partial-restored");
        assert_eq!(replayed, recovered);
        assert_eq!(model.generate_calls.load(Ordering::SeqCst), 1);
        assert_eq!(model.reconcile_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn journal_retry_keys_and_sequences_are_operation_scoped() -> Result<()> {
        let journal = Journal::default();
        let first = OperationId::from_bytes([20; 16]);
        let second = OperationId::from_bytes([21; 16]);
        for (operation_id, digest) in [(first, [1; 32]), (second, [2; 32])] {
            journal
                .append(
                    operation_id,
                    "execution:started".into(),
                    ExecutionEvent::Started {
                        request_digest: digest,
                    },
                )
                .await?;
        }
        assert_eq!(journal.replay(first).await?[0].sequence, 1);
        assert_eq!(journal.replay(second).await?[0].sequence, 1);
        Ok(())
    }
}
