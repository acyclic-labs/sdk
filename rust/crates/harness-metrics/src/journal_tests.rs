//! Journal pages projected through the original executor producers.
use super::{Table, journal_page};
use acyclic_harness_core::{
    AgentId, OperationId, Result,
    conversation::Limits,
    executor::{
        ExecutionEvent, ExecutionJournal, ModelPurpose, ModelTokenUsage, ModelUsageRecord,
        producers::{ModelCallUsage, append_model_observation},
    },
    filesystem::MemoryHarnessStorage,
    model::{
        Model, ModelContent, ModelDispatch, ModelEvent, ModelMessage, ModelRequest, ModelRole,
        PreparedModelRequest,
    },
    test_support::stage_model_request,
};
use arrow_array::{Array, StringArray};

fn prepared(messages: u32) -> Result<PreparedModelRequest> {
    PreparedModelRequest::prepare(
        ModelRequest {
            summary: false,
            compose_tools: false,
            model: Model::new("test".into(), "segmented".into(), "1".into())?,
            messages: (0..messages)
                .map(|index| ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text(format!("turn {}", index + 1)),
                })
                .collect(),
            tools: Vec::new(),
            sampling: Default::default(),
        },
        Limits::default(),
        None,
    )
}

async fn started(journal: &dyn ExecutionJournal, operation: OperationId) -> Result<()> {
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await
}

fn replay_keys(batch: &super::Batch) -> Vec<String> {
    let Some(keys) = batch
        .data
        .column_by_name("replay_key")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
    else {
        return Vec::new();
    };
    (0..keys.len())
        .filter(|&row| !keys.is_null(row))
        .map(|row| keys.value(row).to_owned())
        .collect()
}

/// A model call is keyed by the request content the journal reconstructs from
/// its admission, not by the stored manifest bytes.
#[tokio::test]
async fn model_calls_carry_the_admitted_request_replay_key() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
    let journal = storage.journal();
    let operation = OperationId::new();
    started(journal.as_ref(), operation).await?;
    let prepared = prepared(100)?;
    let request =
        stage_model_request(journal.as_ref(), operation, "request:100", &prepared).await?;
    journal
        .append(
            operation,
            "model:100",
            &ExecutionEvent::ModelStarted {
                step: 100,
                purpose: ModelPurpose::Response,
                request,
                request_digest: prepared.request_digest(),
                context: None,
            },
            None,
        )?
        .await?;
    let expected = blake3::Hash::from(acyclic_harness_core::model::replay_key(prepared.bytes())?)
        .to_hex()
        .to_string();
    let page = journal_page(journal.as_ref(), operation, "agents/generated", 0, |_| None).await?;
    let found = page
        .batches
        .iter()
        .filter(|batch| batch.table == Table::ModelCalls)
        .any(|batch| replay_keys(batch).contains(&expected));
    assert!(found, "export must identify reconstructed request content");
    Ok(())
}

/// Resuming at a page end projects the committed terminal once, without the
/// admission that preceded the cursor.
#[tokio::test]
async fn resumed_pages_project_the_terminal_without_its_prefix() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
    let journal = storage.journal();
    let operation = OperationId::new();
    started(journal.as_ref(), operation).await?;
    let prepared = prepared(1)?;
    let request = stage_model_request(journal.as_ref(), operation, "request:1", &prepared).await?;
    journal
        .append(
            operation,
            "admission",
            &ExecutionEvent::ModelStarted {
                step: 1,
                purpose: ModelPurpose::Response,
                request,
                request_digest: prepared.request_digest(),
                context: None,
            },
            None,
        )?
        .await?;
    let quiet = journal_page(journal.as_ref(), operation, "agents/usage", 0, |_| None).await?;
    let metadata = serde_json::json!({"usage":{"input_tokens":3,"output_tokens":5}});
    let usage = ModelCallUsage {
        version: ModelUsageRecord::VERSION,
        completed: true,
        dispatch: ModelDispatch {
            operation_id: operation,
            step: 1,
            request_digest: prepared.request_digest(),
        },
        purpose: ModelPurpose::Response,
        provider: "test",
        model: "segmented",
        agent_path: None,
        sampling: Default::default(),
        tokens: ModelTokenUsage::reported(&metadata),
        complete_input_tokens: None,
        started_at: None,
        completed_at: None,
        latency_ns: None,
        provider_request_id: None,
        system_fingerprint: None,
        ttft_ns: None,
        cache: None,
        abandoned: Default::default(),
    };
    append_model_observation(
        journal.as_ref(),
        operation,
        "terminal",
        (1, ModelPurpose::Response),
        &ModelEvent::Completed { metadata },
        Some(&usage),
        None,
    )
    .await?;
    let projected = journal_page(journal.as_ref(), operation, "agents/usage", 0, |_| None).await?;
    let resumed = journal_page(
        journal.as_ref(),
        operation,
        "agents/usage",
        quiet.end,
        |_| None,
    )
    .await?;
    assert_eq!(resumed.end, projected.end);
    assert_eq!(resumed.batches.len(), 1);
    let models = projected
        .batches
        .iter()
        .find(|batch| batch.table == Table::ModelCalls)
        .ok_or_else(|| acyclic_harness_core::Error::Invalid("model calls absent".into()))?;
    assert_eq!(models.data.num_rows(), 2);
    assert_eq!(
        resumed.batches[0].data,
        models.data.slice(1, 1),
        "resume must project the same committed observation without its prefix"
    );
    Ok(())
}
