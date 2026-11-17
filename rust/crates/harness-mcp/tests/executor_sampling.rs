//! MCP sampling over the ordinary executor and model boundary.
acyclic_test_support::counting_allocator!();

use acyclic_harness_core::{
    Capabilities, Error, OperationId, Result,
    context::{ContextPipeline, ModelContextCapacity},
    conversation::Limits,
    executor::{StockExecutor, test_support::Journal},
    model::{Model, ModelContent, ModelEvent, ModelProvider},
    runtime::{RuntimeScope, ToolContext, ToolPolicy, ToolPolicyDecision},
    test_support::{FixedToolPolicy, ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
    tool::ToolRegistry,
};
use acyclic_harness_mcp::callbacks::{McpCallbackPolicy, McpCallbackRequest, McpRuntimeCallbacks};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

fn tool_turn(calls: impl IntoIterator<Item = ModelEvent>) -> ScriptedTurn {
    let mut events: Vec<_> = calls.into_iter().map(ScriptedEvent::Event).collect();
    events.push(ScriptedEvent::Event(ModelEvent::Completed {
        metadata: Value::Null,
    }));
    ScriptedTurn::Events(events)
}

fn uncompacted_executor(
    model: Model,
    provider: Arc<dyn ModelProvider>,
    context: ContextPipeline,
    tools: ToolRegistry,
) -> StockExecutor {
    StockExecutor::new(model, provider, context, tools)
        .with_compaction_policy(acyclic_harness_core::context::CompactionPolicy::Disabled)
}

fn allowed() -> acyclic_harness_core::Result<Arc<dyn ToolPolicy>> {
    Ok(Arc::new(FixedToolPolicy::new(ToolPolicyDecision::Allow)?))
}

fn admitted_context(
    scope: RuntimeScope,
) -> acyclic_harness_core::Result<acyclic_harness_core::runtime::TaskContext> {
    acyclic_harness_core::runtime::test_context::agent_context(scope, None)
}

#[tokio::test]
async fn sampling_returns_the_completed_message_instead_of_streamed_prefixes() -> Result<()> {
    use acyclic_harness_mcp::callbacks::{McpCallbackPolicy, McpCallbackRequest, McpRuntimeCallbacks};
    for answer in ["", "final λ\n"] {
        let scope = RuntimeScope::new(Capabilities::new(["model:generate"]), Limits::default())?;
        let context = Arc::new(admitted_context(scope.clone())?);
        let parent = OperationId::new();
        let tool = ToolContext::new(context, parent, "remote")?;
        let model = Arc::new(
            ScriptedModelProvider::new([tool_turn([
                ModelEvent::Content {
                    delta: "older answer".into(),
                },
                ModelEvent::Content {
                    delta: answer.into(),
                },
                ModelEvent::MessageCompleted {
                    text: answer.into(),
                },
            ])])
            .with_context_capacity(ModelContextCapacity {
                context_tokens: 65_536,
                output_tokens: Some(128),
                output_shares_context: false,
            }),
        );
        let callbacks = McpRuntimeCallbacks::new(
            uncompacted_executor(
                Model::new("fixture".into(), "sampling".into(), "1".into())?,
                model.clone(),
                ContextPipeline::default(),
                ToolRegistry::default(),
            )
            .with_tool_authority(scope, allowed()?)?,
            Arc::new(Journal::default()),
            "fixture".into(),
            ModelContent::Text("continue".into()),
            None,
            None,
        )?;
        let request = McpCallbackRequest {
            parent,
            policy: McpCallbackPolicy {
                server: "sample".into(),
                revision: "1".into(),
                sampling: true,
                maximum_requests: 1,
            },
            message: json!({"jsonrpc":"2.0","id":"sample","method":"sampling/createMessage",
                "params":{"messages":[{"role":"user","content":{"type":"text","text":"sample"}}],"maxTokens":128}}),
        };
        let mut bounded = request.clone();
        bounded.message["params"]["maxTokens"] = json!(127);
        assert!(matches!(callbacks.admit(tool.clone(), &bounded).await,
            Err(Error::Unsupported(message)) if message.contains("maxTokens")));
        assert!(model.requests().is_empty());
        let (_, reply) = callbacks.admit(tool, &request).await?;
        assert_eq!(reply.pointer("/result/content/text"), Some(&json!(answer)));
        assert_eq!(model.requests().len(), 1);
    }
    Ok(())
}

fn catalog(count: usize) -> Result<ToolRegistry> {
    let template = acyclic_harness_core::tool::files::read()?;
    let mut tools = ToolRegistry::default();
    for index in 0..count {
        let name = format!("tool_{index:05}");
        for revision in ["1", "2"] {
            let mut tool = template.clone();
            tool.definition.name = name.clone();
            tool.definition.revision = revision.into();
            tools.register((tool, None), &mut None, None)?;
        }
        if index % 2 == 0 {
            tools.remove_from_model(&name)?;
        } else {
            tools.select_model_version(&name, "1")?;
        }
    }
    Ok(tools)
}

fn request(parent: OperationId, id: &str) -> McpCallbackRequest {
    McpCallbackRequest {
        parent,
        policy: McpCallbackPolicy {
            server: "sample".into(),
            revision: "1".into(),
            sampling: true,
            maximum_requests: 4,
        },
        message: json!({"jsonrpc":"2.0","id":id,"method":"sampling/createMessage",
            "params":{"messages":[{"role":"user","content":{"type":"text","text":"sample"}}],"maxTokens":128}}),
    }
}

#[tokio::test]
async fn tool_free_sampling_allocations_are_independent_of_catalog_size() -> Result<()> {
    let mut observations = [Vec::new(), Vec::new()];
    // Both catalogs retain two revisions and alternate hidden/selected names.
    // Exact allocation equality detects copying even one extra schema; 64
    // selected names already make the deliberate full-copy control distinct.
    for count in [10, 128] {
        let admitted_allocations = Arc::new(Mutex::new(None));
        let observed = admitted_allocations.clone();
        let model = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Text("answer".into()),
                ScriptedTurn::Text("answer".into()),
                ScriptedTurn::Text("answer".into()),
            ])
            .with_context_capacity(ModelContextCapacity {
                context_tokens: 65_536,
                output_tokens: Some(128),
                output_shares_context: false,
            })
            .with_dispatch_observer(move |_| {
                // Request admission is complete. Response journaling serializes
                // wall-clock latency, whose decimal width is nondeterministic.
                *observed.lock().unwrap() = acyclic_test_support::current_allocations();
                Box::pin(async { Ok(()) })
            }),
        );
        let scope = RuntimeScope::new(Capabilities::new(["model:generate"]), Limits::default())?;
        let context = Arc::new(admitted_context(scope.clone())?);
        let parent = OperationId::from_bytes([1; 16]);
        let tool = ToolContext::new(context, parent, "remote")?;
        let catalog = catalog(count)?;
        let callbacks = McpRuntimeCallbacks::new(
            uncompacted_executor(
                Model::new("test".into(), "sampling".into(), "1".into())?,
                model.clone(),
                ContextPipeline::default(),
                catalog.clone(),
            )
            .with_tool_authority(scope, allowed()?)?,
            Arc::new(Journal::default()),
            "test".into(),
            ModelContent::Text("continue".into()),
            None,
            None,
        )?;
        callbacks
            .admit(tool.clone(), &request(parent, "warmup"))
            .await?;
        // Warm the synchronous executor's thread-local parking state outside measurement.
        futures::executor::block_on(async {});
        for (index, id) in ["sample", "broken"].into_iter().enumerate() {
            let request = request(parent, id);
            let mut result = None;
            acyclic_test_support::measure(|| {
                result = Some(futures::executor::block_on(async {
                    if index == 1 {
                        // Deliberately restore the former request-assembly bug:
                        // deep-copy the catalog before discarding all tools.
                        std::hint::black_box(catalog.definitions(None)?);
                    }
                    callbacks.admit(tool.clone(), &request).await
                }));
            });
            let (_, reply) = result.expect("sample ran")?;
            assert_eq!(
                reply.pointer("/result/content/text"),
                Some(&json!("answer"))
            );
            let allocations = admitted_allocations
                .lock()
                .unwrap()
                .take()
                .expect("sampling reached provider dispatch inside measurement");
            observations[index].push((allocations.count_total, allocations.bytes_total));
        }
        assert!(
            model
                .requests()
                .iter()
                .all(|request| request.tools.is_empty())
        );
        assert_eq!(model.remaining(), 0);
    }
    let independent = |measurements: &[(u64, u64)]| measurements[0] == measurements[1];
    assert!(
        independent(&observations[0]),
        "sampling copied catalog-sized data: {observations:?}"
    );
    assert!(
        !independent(&observations[1]),
        "detector missed deliberately copied schemas: {observations:?}"
    );
    eprintln!("tool-free sampling (allocations, bytes), 10 versus 128 tools: {observations:?}");
    Ok(())
}

#[test]
fn one_time_install_is_public() {
    // Hosts install a loaded configuration once, without the watched reload path.
    let _ = acyclic_harness_mcp::McpConfig::install;
}
