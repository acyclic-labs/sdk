//! Continuing projection publication through real Stream and private content.
#![cfg(feature = "filesystem-local")]

use acyclic_harness::context::{
    CompactionPolicy, CompactionReference, CompactionRetention, Context, ContextPipeline,
    ContextPlacement, DurableContextProvider, ModelContextCapacity, ModelTokenCount, SourceStage,
};
use acyclic_harness::conversation::{ContentResidencyVerifier, FileRef};
use acyclic_harness::executor::{Executor, StockExecutor, TurnInput};
use acyclic_harness::filesystem::MemoryHarnessStorage;
use acyclic_harness::model::{
    FileProjectionPolicy, Model, ModelAttempt, ModelContent, ModelContentPart, ModelDispatch,
    ModelEvent, ModelMessage, ModelProvider, ModelRole, PreparedModelRequest,
};
use acyclic_harness::tool::ToolRegistry;
use acyclic_harness::{AgentId, Error, OperationId, Result};
use acyclic_stream::{BoxProviderFuture, StreamPath};
use bytes::Bytes;
use std::sync::{Arc, Mutex, atomic::Ordering};

#[path = "support/stream.rs"]
mod stream_fault;
use stream_fault::LostSessionAck;

struct StoredContent(Arc<MemoryHarnessStorage>);

#[derive(Default)]
struct SummaryModel {
    requests: Mutex<Vec<PreparedModelRequest>>,
}

impl ModelProvider for SummaryModel {
    fn context_capacity(&self, _: &Model) -> Result<ModelContextCapacity> {
        Ok(ModelContextCapacity {
            context_tokens: 131_072,
            output_tokens: 4_096,
        })
    }

    fn count_tokens(&self, request: &PreparedModelRequest) -> Result<ModelTokenCount> {
        // This synthetic provider declares byte units including referenced
        // bodies, serialized descriptors and a fixed framing allowance.
        let message_tokens = request
            .request()
            .messages
            .iter()
            .map(|message| {
                let encoded = serde_json::to_vec(message)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let bytes = message.content.file_refs().iter().try_fold(
                    encoded.len() as u64,
                    |total, file| {
                        total
                            .checked_add(file.descriptor().byte_length())
                            .ok_or_else(|| Error::Invalid("synthetic count overflow".into()))
                    },
                )?;
                u32::try_from(bytes)
                    .map_err(|_| Error::Invalid("synthetic count exceeds u32".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ModelTokenCount {
            request_digest: request.manifest().request_digest,
            fixed_tokens: 512,
            message_tokens,
        })
    }

    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
        assert_eq!(dispatch.request_digest, request.manifest().request_digest);
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        Box::pin(futures::stream::iter([
            Ok(ModelEvent::Content {
                delta: "retained summary".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ]))
    }

    fn reconcile<'a>(
        &'a self,
        _: ModelAttempt,
    ) -> BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Err(Error::Unsupported("no uncertain fixture attempt".into())) })
    }
}

impl ContentResidencyVerifier for StoredContent {
    fn verify<'a>(&'a self, file: &'a FileRef) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move { file.descriptor().verify(&self.0.read(file).await?) })
    }
}

async fn message(
    storage: &MemoryHarnessStorage,
    role: ModelRole,
    text: &str,
) -> Result<ModelMessage> {
    let operation = OperationId::new();
    let file = storage
        .stage(
            operation,
            &format!("context/{operation}.txt"),
            text.as_bytes(),
            "text/plain",
            "context.txt",
        )
        .await?;
    Ok(ModelMessage {
        role,
        content: ModelContent::Part(ModelContentPart::File {
            file,
            policy: FileProjectionPolicy::BoundedFull,
        }),
    })
}

struct Fixture {
    stream: Arc<LostSessionAck>,
    storage: Arc<MemoryHarnessStorage>,
    provider: DurableContextProvider,
    source: Context,
    compacted: Context,
    reference: CompactionReference,
}

async fn fixture() -> Result<Fixture> {
    let stream = Arc::new(LostSessionAck::default());
    let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 65_536).await?);
    let provider = DurableContextProvider::new(
        stream.clone(),
        StreamPath::new("context/continuing").map_err(|error| Error::Invalid(error.to_string()))?,
        "memory",
        "1",
        1,
        Arc::new(StoredContent(storage.clone())),
    )?;
    let source = Context {
        messages: vec![
            message(&storage, ModelRole::User, "old").await?,
            message(&storage, ModelRole::Assistant, "answer").await?,
            message(&storage, ModelRole::User, "current").await?,
        ],
        current_input_index: Some(2),
        ..Context::default()
    };
    let mut covered = source.clone();
    covered.messages.truncate(2);
    covered.current_input_index = None;
    let summary = StockExecutor::new(
        Model::new("synthetic", "summary", "1", serde_json::json!({}))?,
        Arc::new(SummaryModel::default()),
        ContextPipeline::default(),
        ToolRegistry::new(),
    )
    .with_compaction_policy(CompactionPolicy::Disabled)
    .with_max_output_tokens(32)?
    .summarize(
        storage.journal().as_ref(),
        OperationId::new(),
        covered,
        ModelContent::Text("Summarize the pinned source".into()),
    )
    .await?;
    let (compacted, reference) =
        DurableContextProvider::compact(&source, 2, Some(summary), CompactionRetention::default())?;
    Ok(Fixture {
        stream,
        storage,
        provider,
        source,
        compacted,
        reference,
    })
}

#[tokio::test]
async fn lost_pair_ack_recovers_both_records_without_exposing_raw_source() -> Result<()> {
    let Fixture {
        stream,
        provider,
        source,
        compacted,
        reference,
        ..
    } = fixture().await?;
    stream.lose_ack.store(true, Ordering::SeqCst);
    let key = Bytes::from_static(b"pair");
    assert!(matches!(
        provider
            .append_compaction(
                0,
                source.clone(),
                compacted.clone(),
                reference.clone(),
                key.clone()
            )
            .await,
        Err(Error::Storage(_))
    ));
    assert_eq!(provider.tail_revision().await?, 2);
    assert_eq!(provider.latest().await?, compacted);
    let pair = provider
        .append_compaction(0, source, compacted.clone(), reference, key)
        .await?;
    assert_eq!(provider.tail_revision().await?, 2);
    let pin = provider
        .latest_revision()
        .await?
        .ok_or_else(|| Error::Storage("missing projection".into()))?;
    assert_eq!(pair.last(), Some(&pin));
    provider
        .append(2, Context::default(), None, Bytes::from_static(b"later"))
        .await?;
    assert_eq!(provider.latest().await?, Context::default());
    assert_eq!(provider.revision(pin.revision).await?, pin);
    Ok(())
}

#[tokio::test]
async fn malformed_pair_receipts_preserve_exact_recovery() -> Result<()> {
    for field in [1, 2, 3] {
        let Fixture {
            stream,
            provider,
            source,
            compacted,
            reference,
            ..
        } = fixture().await?;
        stream.append_receipt_fault.store(field, Ordering::SeqCst);
        let key = Bytes::from_static(b"malformed-pair");
        assert!(matches!(
            provider
                .append_compaction(
                    0,
                    source.clone(),
                    compacted.clone(),
                    reference.clone(),
                    key.clone()
                )
                .await,
            Err(Error::Storage(_))
        ));
        assert_eq!(provider.tail_revision().await?, 2);
        assert_eq!(provider.latest().await?, compacted);
        provider
            .append_compaction(0, source, compacted, reference, key)
            .await?;
        assert_eq!(provider.tail_revision().await?, 2);
    }
    Ok(())
}

#[tokio::test]
async fn retained_summary_composes_into_later_admissions_and_exact_recovery() -> Result<()> {
    let Fixture {
        storage,
        provider,
        source,
        compacted,
        reference,
        ..
    } = fixture().await?;
    provider
        .append_compaction(
            0,
            source,
            compacted.clone(),
            reference,
            Bytes::from_static(b"continuing-source"),
        )
        .await?;
    let provider = Arc::new(provider);
    let model = Arc::new(SummaryModel::default());
    let executor = StockExecutor::new(
        Model::new("synthetic", "continuing", "1", serde_json::json!({}))?,
        model.clone(),
        ContextPipeline::new([Arc::new(SourceStage::new(
            "memory",
            "1",
            provider.clone(),
            ContextPlacement::Prepend,
        ))
            as Arc<dyn acyclic_harness::context::ContextStage>]),
        ToolRegistry::new(),
    );
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("later input".into()),
        selected_context: None,
        max_steps: 1,
    };
    let journal = storage.journal();
    let output = executor.execute(input.clone(), journal.as_ref()).await?;
    {
        let requests = model.requests.lock().unwrap();
        let request = requests
            .first()
            .ok_or_else(|| Error::Invalid("missing later admission".into()))?;
        assert_eq!(
            request.request().messages.len(),
            compacted.messages.len() + 1
        );
        assert_eq!(
            request.request().messages.get(..compacted.messages.len()),
            Some(compacted.messages.as_slice())
        );
        assert_eq!(
            request
                .request()
                .messages
                .last()
                .map(|message| &message.content),
            Some(&input.input)
        );
    }
    provider
        .append(
            2,
            Context::default(),
            None,
            Bytes::from_static(b"changed-source"),
        )
        .await?;
    assert_eq!(executor.execute(input, journal.as_ref()).await?, output);
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    executor
        .execute(
            TurnInput {
                operation_id: OperationId::new(),
                input: ModelContent::Text("new source admission".into()),
                selected_context: None,
                max_steps: 1,
            },
            journal.as_ref(),
        )
        .await?;
    let requests = model.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests
            .last()
            .map(|request| request.request().messages.len()),
        Some(1)
    );
    Ok(())
}
