use super::*;
use super::test_support::Journal;
#[cfg(not(target_arch = "wasm32"))]
mod code_mode;
mod context_review;
#[cfg(not(target_arch = "wasm32"))]
mod feedback;
mod host_invocations;
mod pre_dispatch;
mod saved_admission;
mod step_configuration;

#[cfg(not(target_arch = "wasm32"))]
mod policy_identity;

#[cfg(not(target_arch = "wasm32"))]
mod phase_claim_tests;

#[cfg(not(target_arch = "wasm32"))]
mod publication_observation;

#[cfg(not(target_arch = "wasm32"))]
mod original_preparation;
#[cfg(not(target_arch = "wasm32"))]
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
            output_shares_context: false,
        })
        .with_token_counter(move |request| {
            #[cfg(not(target_arch = "wasm32"))]
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
                                ModelContentPart::Text { text, .. } => text.len() as u64,
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
                _ => {}
            }
            Ok(count)
        });
    (Arc::new(provider), calls)
}

use crate::{
    AgentId, Capabilities,
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

/// The explicit allow-every-call policy of tests that do not exercise
/// approvals: every tool authority names its policy.
fn allowed() -> Result<Arc<dyn crate::runtime::ToolPolicy>> {
    Ok(Arc::new(crate::test_support::FixedToolPolicy::new(
        crate::runtime::ToolPolicyDecision::Allow,
    )?))
}

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
use crate::runtime::test_context::{NoReads, PrivateVolume, agent_context, private_volume};

fn admitted_context(scope: RuntimeScope) -> Result<crate::runtime::TaskContext> {
    agent_context(scope, None)
}

/// The agent's private volume survives context scoping and process restart.
fn context_identity(
    context: &crate::runtime::TaskContext,
) -> Option<&crate::conversation::VolumeRef> {
    context
        .content_binding(None)
        .and_then(|binding| binding.writer.as_ref())
        .map(|writer| writer.volume())
}

struct ContextOnlyTool {
    admission: Option<crate::conversation::VolumeRef>,
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
            assert_eq!(context_identity(context.task()), self.admission.as_ref());
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
            assert_eq!(context_identity(context.task()), self.admission.as_ref());
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
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<(Value, acyclic_fs::NativeCustody)> {
        self.supports_native_projection(work)?;
        Ok((
            serde_json::json!({"kind":"json","value":result.value}),
            None,
        ))
    }
}


mod compaction;
#[cfg(not(target_arch = "wasm32"))]
mod compaction_resolution;
mod content;
#[cfg(not(target_arch = "wasm32"))]
mod context_input_reuse;
mod final_message;
mod invocation_bounds;
mod live_execution;
#[cfg(not(target_arch = "wasm32"))]
mod native_tools;
mod output_bounds;
mod reconciliation_scan;
mod recovery;
mod replay;
mod request_continuation;
#[cfg(not(target_arch = "wasm32"))]
mod request_ownership;
mod summary_authorization;
mod target_replay;
#[cfg(not(target_arch = "wasm32"))]
mod tool_metadata;
mod tools_1;
mod tools_2;
mod transcript_delta;
mod turn_cost;
#[cfg(not(target_arch = "wasm32"))]
mod working_context;
mod working_tail;
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
