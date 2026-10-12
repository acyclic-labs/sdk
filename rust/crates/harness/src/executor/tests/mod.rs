use super::*;

use serde_json::json;

fn uncompacted_executor(
    model: Model,
    provider: Arc<dyn ModelProvider>,
    context: ContextPipeline,
    tools: ToolRegistry,
) -> StockExecutor {
    StockExecutor::new(model, provider, context, tools)
        .with_compaction_policy(crate::context::CompactionPolicy::Disabled)
}

fn fixture_token_count(
    request: &crate::model::PreparedModelRequest,
) -> Result<crate::context::ModelTokenCount> {
    Ok(crate::context::ModelTokenCount {
        request_digest: request.request_digest(),
        fixed_tokens: u32::try_from(request.bytes().len())
            .map_err(|_| Error::Invalid("fixture request exceeds u32".into()))?,
        message_tokens: vec![0; request.request().messages.len()],
    })
}

#[derive(Default)]
struct CountedModel {
    capacity: u32,
    requests: Mutex<Vec<ModelRequest>>,
    interrupt_summary: bool,
    reconciliations: AtomicUsize,
    dispatches: Mutex<Vec<crate::model::ModelDispatch>>,
    accounting_fault: Option<AccountingFault>,
    accounting_calls: AtomicUsize,
}

#[derive(Clone, Copy)]
enum AccountingFault {
    Digest,
    MissingMessage,
    ExtraMessage,
    CompactedPressure,
}

impl ModelProvider for CountedModel {
    fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
        Ok(crate::context::ModelContextCapacity {
            context_tokens: self.capacity,
            output_tokens: 4_096,
        })
    }

    fn count_tokens(
        &self,
        request: &crate::model::PreparedModelRequest,
    ) -> Result<crate::context::ModelTokenCount> {
        self.accounting_calls.fetch_add(1, Ordering::SeqCst);
        let message_tokens =
            request
                .request()
                .messages
                .iter()
                .map(|message| {
                    let text_bytes = match &message.content {
                        ModelContent::Text(text) => text.len() as u64,
                        _ => 0,
                    };
                    let bytes = message.content.parts().iter().try_fold(
                        10 + text_bytes,
                        |total, part| {
                            let count = match part {
                                ModelContentPart::Text { text } => text.len() as u64,
                                ModelContentPart::File { file, .. } => {
                                    file.descriptor().byte_length()
                                }
                                _ => crate::contract::canonical_json_bytes(part)?.len() as u64,
                            };
                            Ok::<_, Error>(total + count)
                        },
                    )?;
                    u32::try_from(bytes)
                        .map_err(|_| Error::Invalid("synthetic token bound exceeds u32".into()))
                })
                .collect::<Result<Vec<_>>>()?;
        let mut count = crate::context::ModelTokenCount {
            request_digest: request.request_digest(),
            fixed_tokens: 100,
            message_tokens,
        };
        match self.accounting_fault {
            Some(AccountingFault::Digest) => count.request_digest[0] ^= 1,
            Some(AccountingFault::MissingMessage) => {
                count.message_tokens.pop();
            }
            Some(AccountingFault::ExtraMessage) => count.message_tokens.push(1),
            Some(AccountingFault::CompactedPressure)
                if request.request().messages.first().is_some_and(|message| {
                    matches!(
                        message.content,
                        ModelContent::Part(ModelContentPart::File { .. })
                    )
                }) =>
            {
                count.fixed_tokens = self.capacity;
            }
            _ => {}
        }
        Ok(count)
    }

    fn generate<'a>(
        &'a self,
        request: crate::model::PreparedModelRequest,
        dispatch: crate::model::ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        let summary = request.request().messages.last().is_some_and(|message| matches!(&message.content, ModelContent::Text(text) if text.starts_with("Summarize the preceding context")));
        self.requests
            .lock()
            .unwrap()
            .push(request.request().clone());
        self.dispatches.lock().unwrap().push(dispatch);
        if summary && self.interrupt_summary {
            return Box::pin(stream::iter([
                Ok(ModelEvent::Content {
                    delta: "partial-".into(),
                }),
                Err(Error::Storage("synthetic summary interruption".into())),
            ]));
        }
        Box::pin(stream::iter([
            Ok(ModelEvent::Content {
                delta: if summary { "summary" } else { "answer" }.into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }

    fn reconcile<'a>(
        &'a self,
        dispatch: crate::model::ModelDispatch,
        observed: &'a [crate::model::ModelEvent],
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            if !self.interrupt_summary {
                return Ok(None);
            }
            assert!(self.dispatches.lock().unwrap().contains(&dispatch));
            assert_eq!(
                observed,
                vec![ModelEvent::Content {
                    delta: "partial-".into()
                }]
            );
            self.reconciliations.fetch_add(1, Ordering::SeqCst);
            Ok(Some(vec![
                ModelEvent::Content {
                    delta: "summary".into(),
                },
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]))
        })
    }
}

struct ChangingCompactionSource(AtomicUsize);

impl crate::context::ContextSource for ChangingCompactionSource {
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        _: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        let revision = self.0.fetch_add(1, Ordering::SeqCst);
        crate::conversation::BoundedContentFuture::new(io, || async move {
            Ok(vec![
                ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(format!("keep-{revision}")),
                },
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("o".repeat(28_000)),
                },
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Text("a".repeat(28_000)),
                },
            ])
        })
    }
}

use crate::{
    AgentId, Capabilities, Outcome,
    conversation::{FileDescriptor, VolumeOwner, VolumeRef},
    resources::ProviderRef,
    tool::{Tool, ToolDefinition, ToolExecutor},
};

use futures::{FutureExt as _, stream};

use std::collections::HashMap;

use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct ScriptedModel {
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    dispatches: Mutex<Vec<crate::model::ModelDispatch>>,
    reconciliations: AtomicUsize,
    responses: Vec<Vec<std::result::Result<ModelEvent, String>>>,
    recovery: Option<(Vec<ModelEvent>, Vec<ModelEvent>)>,
}

fn echo_call(call_id: &str, arguments: Value) -> ModelEvent {
    ModelEvent::ToolCall {
        call_id: call_id.into(),
        name: "example.echo".into(),
        arguments,
    }
}

impl Default for ScriptedModel {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            dispatches: Mutex::new(Vec::new()),
            reconciliations: AtomicUsize::new(0),
            responses: vec![
                vec![
                    Ok(echo_call("call-1", json!({"value":"hello"}))),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ],
                vec![
                    Ok(ModelEvent::Content {
                        delta: "done".into(),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: json!({"finish":"stop"}),
                    }),
                ],
            ],
            recovery: None,
        }
    }
}

impl ScriptedModel {
    fn slipping() -> Self {
        let mut model = Self::default();
        model.responses.insert(
            0,
            vec![
                Ok(echo_call("call-1", json!({"parameters":{"value":"hello"}}))),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ],
        );
        model.responses[1][0] = Ok(echo_call("call-2", json!({"value":"hello"})));
        model
    }

    fn recoverable() -> Self {
        let observed = vec![
            ModelEvent::Content {
                delta: "partial-".into()
            };
            70
        ];
        let mut response: Vec<_> = observed.iter().cloned().map(Ok).collect();
        response.push(Err("stream interrupted".into()));
        Self {
            responses: vec![response],
            recovery: Some((
                observed,
                vec![
                    ModelEvent::Content {
                        delta: "restored".into(),
                    },
                    ModelEvent::Completed {
                        metadata: json!({"finish":"stop"}),
                    },
                ],
            )),
            ..Self::default()
        }
    }
}

async fn capture_admitted_context(scope: RuntimeScope) -> Result<crate::runtime::TaskContext> {
    let captured = Arc::new(Mutex::new(None));
    let sink = captured.clone();
    let mut tasks = crate::runtime::TaskRegistry::default();
    tasks.register(crate::runtime::TaskDefinition::live(
        "context-probe",
        "1",
        move |context, _: ()| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(context);
                Ok(())
            }
        },
    )?)?;
    let harness = crate::runtime::Bindings {
        tasks,
        tools: ToolRegistry::default(),
        scope,
        concurrency: 1,
        ..crate::runtime::Bindings::local()
    }
    .build(None)?;
    let definition = harness.task::<(), ()>("context-probe@1")?;
    assert!(matches!(
        harness.spawn(&definition, ()).await?.result().await?,
        Outcome::Succeeded(())
    ));
    captured
        .lock()
        .unwrap()
        .take()
        .ok_or_else(|| Error::NotFound("admitted context".into()))
}

struct ContextOnlyTool {
    admission: OperationId,
    calls: AtomicUsize,
    reconciles: AtomicUsize,
}

impl ToolExecutor for ContextOnlyTool {
    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        async { Err(Error::Unsupported("original context required".into())) }.boxed()
    }

    fn execute_with_context<'a>(
        &'a self,
        context: crate::runtime::ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        async move {
            assert_eq!(context.task().id(), self.admission);
            assert_eq!(context.operation_id(), invocation.operation_id);
            assert_eq!(context.call_id(), invocation.call_id);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(Error::Storage("response lost after effect".into()))
        }
        .boxed()
    }

    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        async { Err(Error::Unsupported("original context required".into())) }.boxed()
    }

    fn reconcile_with_context<'a>(
        &'a self,
        context: crate::runtime::ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        async move {
            assert_eq!(context.task().id(), self.admission);
            assert_eq!(context.operation_id(), invocation.operation_id);
            assert_eq!(context.call_id(), invocation.call_id);
            self.reconciles.fetch_add(1, Ordering::SeqCst);
            Ok(Some(ToolResult {
                value: json!({"value":"hello"}),
            }))
        }
        .boxed()
    }
}

impl ModelProvider for ScriptedModel {
    fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
        Ok(crate::context::ModelContextCapacity {
            context_tokens: 131_072,
            output_tokens: 4_096,
        })
    }

    fn count_tokens(
        &self,
        request: &crate::model::PreparedModelRequest,
    ) -> Result<crate::context::ModelTokenCount> {
        fixture_token_count(request)
    }

    fn generate<'a>(
        &'a self,
        request: crate::model::PreparedModelRequest,
        dispatch: crate::model::ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        assert_eq!(
            request.bytes(),
            crate::contract::canonical_json_bytes(request.request()).unwrap_or_default()
        );
        assert_eq!(
            request.request_digest(),
            *blake3::hash(request.bytes()).as_bytes()
        );
        assert_eq!(dispatch.request_digest, request.request_digest());
        self.dispatches.lock().unwrap().push(dispatch);
        self.requests
            .lock()
            .unwrap()
            .push(request.request().clone());
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let events = &self.responses[call.min(self.responses.len() - 1)];
        Box::pin(stream::iter(
            events
                .iter()
                .cloned()
                .map(|event| event.map_err(Error::Storage)),
        ))
    }

    fn reconcile<'a>(
        &'a self,
        dispatch: crate::model::ModelDispatch,
        observed: &'a [ModelEvent],
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        let Some((expected, response)) = &self.recovery else {
            return Box::pin(async { Ok(None) });
        };
        assert!(self.dispatches.lock().unwrap().contains(&dispatch));
        self.reconciliations.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if observed != expected {
                return Err(Error::Conflict("unexpected model event prefix".into()));
            }
            Ok(Some(response.clone()))
        })
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
    fn project(
        &self,
        _: &ToolInvocation,
        result: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        self.supports_native_projection(work)?;
        Ok((
            serde_json::json!({"kind":"json","value":result.value}),
            None,
        ))
    }
}

#[derive(Default)]
struct Journal(
    Mutex<Vec<ExecutionRecord>>,
    Mutex<HashMap<String, (FileRef, acyclic_stream::OwnedBytes)>>,
    Mutex<HashMap<InteractionId, (Interaction, Option<InteractionOutcome>)>>,
    AtomicUsize,
    AtomicUsize,
);

impl Journal {
    fn stage_source<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        bytes: acyclic_stream::OwnedBytes,
        media_type: &'static str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        async move {
            let key = format!("{operation_id}:{idempotency_key}");
            let mut stored = self.1.lock().unwrap();
            if let Some((reference, existing)) = stored.get(&key) {
                return if existing.as_ref() == bytes.as_ref() {
                    Ok(reference.clone())
                } else {
                    Err(Error::Conflict("journal staging identity reused".into()))
                };
            }
            let volume = VolumeRef::new(
                ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
                "journal".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([0; 16])),
                &mut None,
            )?;
            let reference = FileRef::new(
                volume,
                format!("journal/{}.json", blake3::hash(key.as_bytes()).to_hex()),
                blake3::hash(&bytes).to_hex().to_string(),
                FileDescriptor::from_bytes(&bytes, media_type, &mut None)?,
                "event.json".into(),
                &mut None,
            )?;
            stored.insert(key, (reference.clone(), bytes));
            Ok(reference)
        }
        .boxed()
    }
}

struct ChangingContextSource(AtomicUsize);

impl crate::context::ContextSource for ChangingContextSource {
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        _: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        let read = self.0.fetch_add(1, Ordering::SeqCst);
        crate::conversation::BoundedContentFuture::new(io, || async move {
            Ok(vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(format!("source revision {read}")),
            }])
        })
    }
}

impl ExecutionJournal for Journal {
    fn verify_model_content<'a>(
        &'a self,
        content: &'a ModelContent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if io.is_some() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "finite test journal verification is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            for file in content.file_refs() {
                file.descriptor()
                    .verify(&self.load(file, io)?.await?, &mut None)?;
            }
            Ok(())
        })
    }

    fn verify_input_file<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if io.is_some() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "finite test journal verification is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let bytes = self.load(reference, io)?.await?;
            reference.descriptor().verify(&bytes, &mut None)
        })
    }

    fn replay<'a>(
        &'a self,
        operation_id: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        if io.is_some() {
            return Err(crate::Error::Unsupported(
                "finite test journal IO is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.3.fetch_add(1, Ordering::SeqCst);
            validate_execution_page(maximum)?;
            Ok(self
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|record| record.operation_id == operation_id && record.sequence > after)
                .take(maximum as usize)
                .cloned()
                .collect())
        })
    }

    fn append<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if io.is_some() {
            return Err(crate::Error::Unsupported(
                "finite test journal append is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let event = event.clone();
            let idempotency_key = idempotency_key.to_owned();
            let mut records = self.0.lock().unwrap();
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
        })
    }

    fn append_if_tail<'a>(
        &'a self,
        operation_id: OperationId,
        expected_tail: u64,
        idempotency_key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        if io.is_some() {
            return Err(crate::Error::Unsupported(
                "finite test journal append is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let event = event.clone();
            let idempotency_key = idempotency_key.to_owned();
            let mut records = self.0.lock().unwrap();
            let tail = records
                .iter()
                .filter(|record| record.operation_id == operation_id)
                .count() as u64;
            if tail != expected_tail {
                return Ok(false);
            }
            records.push(ExecutionRecord {
                operation_id,
                sequence: tail + 1,
                idempotency_key,
                event,
            });
            Ok(true)
        })
    }

    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
        media_type: &'static str,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        if io.is_some() {
            return Err(crate::Error::Unsupported(
                "finite test journal stage is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.stage_source(
                operation_id,
                idempotency_key.to_owned(),
                bytes.to_vec().into(),
                media_type,
            )
            .await
        })
    }

    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        if io.is_some() {
            return Err(crate::Error::Unsupported(
                "finite test journal IO is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.4.fetch_add(1, Ordering::SeqCst);
            self.1
                .lock()
                .unwrap()
                .values()
                .find(|(stored, _)| stored == reference)
                .map(|(_, bytes)| bytes.clone())
                .ok_or_else(|| Error::NotFound("journal content is missing".into()))
        })
    }

    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        request: Interaction,
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            request.validate(&mut None)?;
            let mut interactions = self.2.lock().unwrap();
            if let Some((original, _)) = interactions.get(&id) {
                if original != &request {
                    return Err(Error::Conflict("approval request changed".into()));
                }
            } else {
                interactions.insert(id, (request, None));
            }
            Ok(())
        }
        .boxed()
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>> {
        async move {
            Ok(self
                .2
                .lock()
                .unwrap()
                .get(&id)
                .and_then(|(_, outcome)| outcome.clone()))
        }
        .boxed()
    }
}

impl crate::conversation::ContentResidencyVerifier for Journal {
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        _maximum_bytes: u64,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(
            io,
            || async move { self.load(file, io)?.await },
        )
    }
}

#[derive(Default)]
struct PrefixBoundaryModel(Mutex<Vec<Vec<u8>>>);

impl ModelProvider for PrefixBoundaryModel {
    fn generate<'a>(
        &'a self,
        request: crate::model::PreparedModelRequest,
        _dispatch: crate::model::ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        self.0.lock().unwrap().push(request.bytes().to_vec());
        Box::pin(stream::iter([Ok(ModelEvent::Completed {
            metadata: Value::Null,
        })]))
    }
}

struct OnceStage(AtomicUsize);

impl crate::context::ContextStage for OnceStage {
    fn name(&self) -> &str {
        "once"
    }
    fn contract(&self, _: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        Ok(json!({"revision": "1"}))
    }
    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        context: crate::context::Context,
        _: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, crate::context::Context>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if self.0.fetch_add(1, Ordering::SeqCst) != 0 {
                return Err(Error::NotFound("source removed after admission".into()));
            }
            Ok(context)
        })
    }
}

mod compaction;
mod content;
mod recovery;
mod replay;
mod tools_1;
mod tools_2;
