use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod original_preparation;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod preparation_differential;

#[cfg(not(target_arch = "wasm32"))]
mod sampling_allocations;

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
    Ok(crate::context::ModelTokenCount::from_upper_bounds(
        request.request_digest(),
        u32::try_from(request.bytes().len())
            .map_err(|_| Error::Invalid("fixture request exceeds u32".into()))?,
        vec![0; request.request().messages.len()],
    ))
}

#[derive(Clone, Copy)]
enum AccountingFault {
    Digest,
    MissingMessage,
    ExtraMessage,
    CompactedPressure,
}

fn counted_model(
    capacity: u32,
    fault: Option<AccountingFault>,
    turns: impl IntoIterator<Item = ScriptedTurn>,
    recoveries: impl IntoIterator<Item = ScriptedReconciliation>,
) -> (Arc<ScriptedModelProvider>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let provider = ScriptedModelProvider::new(turns)
        .with_reconciliations(recoveries)
        .with_context_capacity(crate::context::ModelContextCapacity {
            context_tokens: capacity,
            output_tokens: Some(4_096),
        })
        .with_token_counter(move |request| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            acyclic_test_support::differential::record((
                "count tokens",
                request.bytes().len() as u64,
            ));
            counter.fetch_add(1, Ordering::SeqCst);
            let message_tokens = request
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
            let mut count = crate::context::ModelTokenCount::from_upper_bounds(
                request.request_digest(),
                100,
                message_tokens,
            );
            match fault {
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
                    count.input_tokens = u64::from(capacity);
                    count.fixed_tokens = capacity;
                }
                _ => {}
            }
            Ok(count)
        });
    (Arc::new(provider), calls)
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
    tool::ToolExecutor,
};

use std::collections::HashMap;

use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crate::test_support::{
    ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn,
};

fn echo_call(call_id: &str, arguments: Value) -> ModelEvent {
    ModelEvent::ToolCall(crate::model::ToolCall::new(
        call_id.into(),
        "example.echo".into(),
        arguments,
    ))
}

fn tool_turn(calls: impl IntoIterator<Item = ModelEvent>) -> ScriptedTurn {
    let mut events: Vec<_> = calls.into_iter().map(ScriptedEvent::Event).collect();
    events.push(ScriptedEvent::Event(ModelEvent::Completed {
        metadata: Value::Null,
    }));
    ScriptedTurn::Events(events)
}

fn done_turn() -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::Content {
            delta: "done".into(),
        }),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({"finish":"stop"}),
        }),
    ])
}

fn scripted_model() -> ScriptedModelProvider {
    ScriptedModelProvider::new([
        tool_turn([echo_call("call-1", json!({"value":"hello"}))]),
        done_turn(),
    ])
    .with_token_counter(fixture_token_count)
}

fn slipping_model() -> ScriptedModelProvider {
    ScriptedModelProvider::new([
        tool_turn([echo_call("call-1", json!({"parameters":{"value":"hello"}}))]),
        tool_turn([echo_call("call-2", json!({"value":"hello"}))]),
        done_turn(),
    ])
    .with_token_counter(fixture_token_count)
}

fn recoverable_model(attempts: usize) -> ScriptedModelProvider {
    let observed = vec![
        ModelEvent::Content {
            delta: "partial-".into()
        };
        70
    ];
    let turns = (0..attempts)
        .map(|_| {
            let mut events: Vec<_> = observed.iter().cloned().map(ScriptedEvent::Event).collect();
            events.push(ScriptedEvent::Error {
                error: "stream interrupted".into(),
            });
            ScriptedTurn::Events(events)
        })
        .collect::<Vec<_>>();
    ScriptedModelProvider::new(turns)
        .with_reconciliations((0..attempts).map(|_| ScriptedReconciliation {
            orphan: false,
            observed: observed.clone(),
            continuation: Some(vec![
                ModelEvent::Content {
                    delta: "restored".into(),
                },
                ModelEvent::Completed {
                    metadata: json!({"finish":"stop"}),
                },
            ]),
        }))
        .with_token_counter(fixture_token_count)
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
    lost_response: fn() -> Error,
    calls: AtomicUsize,
    reconciles: AtomicUsize,
}

impl ToolExecutor for ContextOnlyTool {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        let Some(context) = context else {
            return Box::pin(async { Err(Error::Unsupported("original context required".into())) });
        };
        Box::pin(async move {
            assert_eq!(context.task().id(), self.admission);
            assert_eq!(context.operation_id(), invocation.operation_id);
            assert_eq!(context.call_id(), invocation.call_id);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err((self.lost_response)())
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        let Some(context) = context else {
            return Box::pin(async { Err(Error::Unsupported("original context required".into())) });
        };
        Box::pin(async move {
            assert_eq!(context.task().id(), self.admission);
            assert_eq!(context.operation_id(), invocation.operation_id);
            assert_eq!(context.call_id(), invocation.call_id);
            self.reconciles.fetch_add(1, Ordering::SeqCst);
            Ok(Some(ToolResult {
                value: json!({"value":"hello"}),
            }))
        })
    }
}

struct FakeTool(AtomicUsize);

impl crate::tool::ToolExecutor for FakeTool {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ToolResult {
                value: invocation.arguments,
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
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
        Box::pin(async move {
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
        })
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
        _context: Option<&'a crate::runtime::TaskContext>,
        _summary_files: &'a [&'a FileRef],
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
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        acyclic_test_support::differential::record(("journal append", expected_tail));
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
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        acyclic_test_support::differential::record(("journal stage", bytes.len() as u64));
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
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        acyclic_test_support::differential::record((
            "journal load",
            reference.descriptor().byte_length(),
        ));
        if io.is_some() {
            return Err(crate::Error::Unsupported(
                "finite test journal IO is unavailable".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.4.fetch_add(1, Ordering::SeqCst);
            if reference.descriptor().media_type() == "text/plain" {
                self.5.fetch_add(1, Ordering::SeqCst);
            }
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
        request: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "test journal has no native interactions".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            request.validate(&mut None)?;
            let mut interactions = self.2.lock().unwrap();
            if let Some((original, _)) = interactions.get(&id) {
                if original != request {
                    return Err(Error::Conflict("approval request changed".into()));
                }
            } else {
                interactions.insert(id, (request.clone(), None));
            }
            Ok(())
        })
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "test journal has no native interactions".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            Ok(self
                .2
                .lock()
                .unwrap()
                .get(&id)
                .and_then(|(_, outcome)| outcome.clone()))
        })
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
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod compaction_resolution;
mod content;
mod invocation_bounds;
mod output_bounds;
mod recovery;
mod replay;
#[cfg(not(target_arch = "wasm32"))]
mod request_ownership;
#[cfg(feature = "filesystem")]
mod summary_authorization;
mod summary_file;
#[cfg(not(target_arch = "wasm32"))]
mod tool_metadata;
mod tools_1;
mod tools_2;
#[cfg(feature = "filesystem")]
mod reconciliation_scan;
fn echo_tools(
    executor: Arc<dyn crate::tool::ToolExecutor>,
    input_schema: Value,
    output_schema: Value,
    projection_schema: Value,
) -> Result<ToolRegistry> {
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            crate::tool::Tool {
                hook_names: &[],
                definition: crate::tool::ToolDefinition {
                    name: "example.echo".into(),
                    revision: "1".into(),
                    description: "Echo".into(),
                    input_schema,
                    output_schema,
                    projection_schema,
                },
                executor,
                projection: Arc::new(Projection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    Ok(tools)
}

fn call_message(invocation: &ToolInvocation) -> ModelMessage {
    ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall(crate::model::ToolCall::new(
            invocation.call_id.clone(),
            invocation.name.clone(),
            invocation.arguments.clone(),
        ))),
    }
}
