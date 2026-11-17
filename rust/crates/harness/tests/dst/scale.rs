//! J1: the context manager's summary request receives a valid rewrite that
//! names a complete tool-exchange boundary, including parallel calls.
use super::*;
use acyclic_harness::{
    AgentId, OperationId,
    context::{Context, ContextPipeline},
    executor::StockExecutor,
    filesystem::MemoryHarnessStorage,
    model::{
        Model, ModelContent, ModelContentPart, ModelMessage, ModelRole, ToolCall,
        ToolResultContent,
    },
    tool::ToolRegistry,
};
use family::{Generator, Variant};
use oracle::{Plan, service, simulate, tag};
use std::sync::Arc;

pub(super) const GENERATOR: Generator = Generator {
    class: "J1",
    seeds: summary_rewrite_is_valid::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = summary_rewrite_is_valid,
    values: u64,
    runner = super::run,
    cases = [seed_0 => 0],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn plan() -> Plan {
    Plan {
        tag: "scale",
        steps: Vec::new(),
        fail_once: None,
    }
}

fn text(role: ModelRole, body: &str) -> ModelMessage {
    ModelMessage {
        role,
        content: ModelContent::Text(body.into()),
    }
}

fn call(id: &str) -> ModelMessage {
    ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
            id.into(),
            "write".into(),
            serde_json::json!({}),
        ))),
    }
}

fn result(id: &str) -> ModelMessage {
    ModelMessage {
        role: ModelRole::Tool,
        content: ModelContent::Part(ModelContentPart::ToolResult {
            call_id: id.into(),
            name: "write".into(),
            content: ToolResultContent::Json {
                value: serde_json::json!({"ok": true}),
            },
        }),
    }
}

#[derive(serde::Deserialize)]
struct Rewrite {
    version: u32,
    drop_through: u32,
    summary: String,
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    simulate(seed, || {
        entry(async move {
            let provider = Arc::new(service(vec![plan()]));
            let executor = StockExecutor::new(
                Model::new("fixture".into(), "scale".into(), "1".into())
                    .map_err(oracle::failed)?,
                provider,
                ContextPipeline::default(),
                ToolRegistry::default(),
            );
            let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536)
                .await
                .map_err(oracle::failed)?;
            // Two calls admit together, then their results. The rewrite must
            // wait for both results before it drops the exchange.
            let source = Context {
                messages: vec![
                    text(ModelRole::System, "instructions"),
                    text(ModelRole::User, &tag("scale")),
                    call("a"),
                    call("b"),
                    result("a"),
                    result("b"),
                ],
                current_input_index: None,
                metadata: Default::default(),
            };
            let summary = executor
                .summarize(
                    storage.journal().as_ref(),
                    OperationId::new(),
                    source,
                    ModelContent::Text("summarize the retained history".into()),
                )
                .await
                .map_err(oracle::failed)?;
            let bytes = storage
                .journal()
                .load(&summary.output, None)
                .map_err(oracle::failed)?
                .await
                .map_err(oracle::failed)?;
            let rewrite: Rewrite = serde_json::from_slice(&bytes).map_err(oracle::failed)?;
            if rewrite.version != 1 || rewrite.drop_through != 6 || rewrite.summary.is_empty() {
                return Err(format!(
                    "invalid summary rewrite: version {}, drop_through {}, summary length {}",
                    rewrite.version,
                    rewrite.drop_through,
                    rewrite.summary.len()
                ));
            }
            Ok(true)
        })
    })
}

fn variants(_seed: u64) -> Vec<Variant> {
    Vec::new()
}
