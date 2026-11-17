//! J1: a context-manager summary request receives a rewrite the manager can
//! validate, so the turn continues instead of failing on the rewrite.
use super::*;
use acyclic_harness::{
    AgentId, OperationId,
    context::{CapacityCompaction, CompactionPolicy, ContextPipeline, ModelContextCapacity},
    executor::{StockExecutor, TurnInput},
    filesystem::MemoryHarnessStorage,
    model::{Model, ModelContent, ModelMessage, ModelRole},
    tool::ToolRegistry,
};
use family::{Generator, Variant};
use oracle::{Plan, service, simulate, tag};
use std::sync::Arc;

pub(super) const GENERATOR: Generator = Generator {
    class: "J1",
    seeds: context_manager_rewrite_is_valid::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = context_manager_rewrite_is_valid,
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

fn world(seed: u64) -> std::result::Result<bool, String> {
    simulate(seed, || {
        entry(async move {
            let provider = Arc::new(service(vec![plan()]).with_context_capacity(
                ModelContextCapacity {
                    context_tokens: 16384,
                    output_tokens: Some(1024),
                    output_shares_context: false,
                },
            ));
            let executor = StockExecutor::new(
                Model::new("fixture".into(), "scale".into(), "1".into())
                    .map_err(oracle::failed)?,
                provider,
                ContextPipeline::default(),
                ToolRegistry::default(),
            )
            .with_compaction_policy(CompactionPolicy::Capacity(CapacityCompaction::default()));
            let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536)
                .await
                .map_err(oracle::failed)?;
            // The source fits the summary window; the large current input does
            // not, so the manager asks for a rewrite before dispatching it.
            let prior = vec![
                text(ModelRole::System, "instructions"),
                text(
                    ModelRole::User,
                    &format!("{} {}", tag("scale"), "x".repeat(8 * 1024)),
                ),
                text(ModelRole::User, "settled history"),
            ];
            let input = TurnInput {
                operation_id: OperationId::new(),
                input: ModelContent::Text("x".repeat(8 * 1024)),
                selected_context: None,
                max_steps: Some(1),
            };
            executor
                .model_step(storage.journal().as_ref(), &input, 0, &prior)
                .await
                .map_err(oracle::failed)?;
            Ok(true)
        })
    })
}

fn variants(_seed: u64) -> Vec<Variant> {
    Vec::new()
}
