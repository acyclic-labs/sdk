//! Captured provider evidence through the original executor, journal and exporters.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_harness_core::{
    AgentId, OperationId,
    context::{CompactionPolicy, ContextPipeline},
    executor::{
        ExecutionEvent, ExecutionPayloadField, Executor as _, StockExecutor, TurnInput, metrics,
        usage_page,
    },
    filesystem::MemoryHarnessStorage,
    model::{ModelContent, ModelRole},
    tool::ToolRegistry,
};
use acyclic_harness_providers::{Credentials, Registry};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::Write,
    sync::{Arc, atomic::Ordering},
};

#[path = "support/transport.rs"]
mod transport;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// A writer whose original byte admission ends before the shard does.
struct ShortWriter {
    bytes: Vec<u8>,
    remaining: usize,
}

impl Write for ShortWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        let count = bytes.len().min(self.remaining);
        self.bytes.extend_from_slice(&bytes[..count]);
        self.remaining -= count;
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn captured_terminal_exports_and_interrupted_sources_remain_distinct() -> TestResult {
    const SUCCESS: &str = include_str!("../src/fixtures/gemini-captured-session.sse");
    // Cut the original response after admitted text, before its terminal frame.
    let cut = SUCCESS.find("thoughtSignature").expect("captured terminal");
    let (port, accepted) = transport::serve(vec![
        transport::Reply::Full(200, SUCCESS),
        transport::Reply::Cut(200, SUCCESS, cut),
    ])
    .await?;
    let mut registry = Registry::new(Credentials::explicit(BTreeMap::from([(
        "gemini".into(),
        "local-replay-only".into(),
    )])))?;
    let mut endpoint = registry.catalog()["gemini"].as_ref().clone();
    endpoint.base_url = format!("http://127.0.0.1:{port}");
    registry.register("gemini".into(), endpoint)?;
    let provider = Arc::new(registry.resolve("gemini/gemini-3.8-flash")?);
    let executor = StockExecutor::new(
        provider.model().clone(),
        provider,
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_compaction_policy(CompactionPolicy::Disabled);
    let agent = AgentId::new();
    let agent_path = agent.to_string();
    let storage = MemoryHarnessStorage::new(agent, 1_048_576).await?;
    let journal = storage.journal();
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("Preserve the captured terminal in offline files".into()),
        selected_context: None,
        max_steps: Some(1),
    };
    let output = executor.execute(input.clone(), journal.as_ref()).await?;
    let records = journal.replay(input.operation_id, 0, 32, None)?.await?;
    assert!(
        records.len() < 32,
        "the complete one-step source fits this page"
    );
    let admission = records
        .iter()
        .find(|record| matches!(record.event, ExecutionEvent::ModelStarted { .. }))
        .expect("original model admission");
    let terminal = records
        .iter()
        .find(|record| {
            matches!(
                record.event,
                ExecutionEvent::Model { usage: Some(_), .. }
                    | ExecutionEvent::ModelInline { usage: Some(_), .. }
            )
        })
        .expect("original measured terminal");
    let request =
        ExecutionPayloadField::admitted_request(journal.as_ref(), &admission.event).await?;
    let call = metrics::inspect::Call::new(
        metrics::inspect::Source {
            scope: &agent_path,
            agent_path: &agent_path,
            operation: input.operation_id,
            source_stream: "journal",
        },
        admission,
        request.bytes(),
    )?;
    let settlement = call.settlement(journal.as_ref(), terminal).await?;
    let response = &output
        .transcript
        .iter()
        .rev()
        .find(|message| message.role == ModelRole::Assistant)
        .expect("original settled assistant message")
        .content;
    let mut inspect = Vec::new();
    settlement.write(&mut inspect, "2026-11-02T00:00:00Z", response)?;
    let log: Value = serde_json::from_slice(&inspect)?;
    let metadata = &log["samples"][0]["metadata"];
    assert_eq!(metadata["terminal_metadata"], output.metadata);
    assert_eq!(metadata["terminal_sequence"], terminal.sequence);
    let usage = usage_page(journal.as_ref(), input.operation_id, &agent_path, 0).await?;
    let usage = usage
        .items
        .iter()
        .find(|row| row.completed)
        .expect("original usage");
    assert_eq!(metadata["usage"], serde_json::to_value(usage)?);
    assert_eq!(metadata["usage"]["tokens"]["input"], 7467);
    assert_eq!(metadata["usage"]["tokens"]["output"], 31);
    assert!(metadata["usage"]["tokens"]["cached"].is_null());
    assert!(metadata["usage"]["provider_request_id"].is_null());
    assert_eq!(
        metadata["usage"]["request_body"],
        output.metadata["request_body"]
    );
    assert!(usage.request_body.is_some());

    let page = metrics::journal_page(journal.as_ref(), input.operation_id, &agent_path, 0, |_| {
        None
    })
    .await?;
    assert!(page.next.is_none());
    let models = page
        .batches
        .iter()
        .find(|batch| batch.table == metrics::Table::ModelCalls)
        .expect("original model shard");
    assert_eq!(
        models.data.num_rows(),
        2,
        "admission and terminal stay separate"
    );
    let column = |name| models.data.column_by_name(name).expect("original column");
    for name in [
        "input_tokens",
        "output_tokens",
        "request_body_digest",
        "request_body_bytes",
    ] {
        assert!(column(name).is_null(0), "admission has unknown {name}");
        assert!(!column(name).is_null(1), "terminal retains {name}");
    }
    assert_eq!(column("cached_tokens").null_count(), 2);
    assert_eq!(column("provider_request_id").null_count(), 2);
    for format in [metrics::Format::Arrow, metrics::Format::Parquet] {
        let mut shard = Vec::new();
        metrics::write(models, &mut shard, format)?;
        assert!(!shard.is_empty());
        let mut short = ShortWriter {
            bytes: Vec::new(),
            remaining: shard.len() / 2,
        };
        assert!(metrics::write(models, &mut short, format).is_err());
        assert!(!short.bytes.is_empty());
    }
    let mut short = ShortWriter {
        bytes: Vec::new(),
        remaining: inspect.len() / 2,
    };
    assert!(
        settlement
            .write(&mut short, "2026-11-02T00:00:00Z", response)
            .is_err()
    );
    assert!(serde_json::from_slice::<Value>(&short.bytes).is_err());

    let interrupted = TurnInput {
        operation_id: OperationId::new(),
        ..input
    };
    assert!(
        executor
            .execute(interrupted.clone(), journal.as_ref())
            .await
            .is_err()
    );
    let usage = usage_page(journal.as_ref(), interrupted.operation_id, &agent_path, 0).await?;
    assert!(usage.items.iter().all(|row| !row.completed));
    let records = journal
        .replay(interrupted.operation_id, 0, 32, None)?
        .await?;
    let admission = records
        .iter()
        .find(|record| matches!(record.event, ExecutionEvent::ModelStarted { .. }))
        .expect("interrupted original admission");
    let request =
        ExecutionPayloadField::admitted_request(journal.as_ref(), &admission.event).await?;
    let call = metrics::inspect::Call::new(
        metrics::inspect::Source {
            scope: &agent_path,
            agent_path: &agent_path,
            operation: interrupted.operation_id,
            source_stream: "journal",
        },
        admission,
        request.bytes(),
    )?;
    let partial = records
        .iter()
        .rev()
        .find(|record| {
            matches!(
                record.event,
                ExecutionEvent::Model { .. } | ExecutionEvent::ModelInline { .. }
            )
        })
        .expect("original text before interruption");
    assert!(call.settlement(journal.as_ref(), partial).await.is_err());
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        2,
        "no resend after response acceptance"
    );
    Ok(())
}
