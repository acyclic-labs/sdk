//! Continuing projection publication through real Stream and private content.
#![cfg(feature = "filesystem-local")]

use acyclic_harness::context::{
    CompactionPolicy, CompactionReference, CompactionRetention, Context, ContextPipeline,
    ContextPlacement, DurableContextProvider, ModelContextCapacity, ModelTokenCount, SourceStage,
};
use acyclic_harness::conversation::{
    ContentPublisher, ContentResidencyVerifier, FileRef, Limits, VolumeRef,
};
use acyclic_harness::executor::{Executor, StockExecutor, TurnInput};
use acyclic_harness::filesystem::MemoryHarnessStorage;
use acyclic_harness::model::{
    FileProjectionPolicy, Model, ModelAttempt, ModelContent, ModelContentPart, ModelDispatch,
    ModelEvent, ModelMessage, ModelProvider, ModelRequest, ModelRole, PreparedModelRequest,
};
use acyclic_harness::tool::{ToolDefinition, ToolRegistry};
use acyclic_harness::{AgentId, Error, OperationId, Result};
use acyclic_stream::{
    BoxProviderFuture, MAX_RECORD_BYTES, ReadRequest, StreamPath, StreamProvider,
};
use bytes::Bytes;
use futures::StreamExt as _;
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

    fn read<'a>(&'a self, file: &'a FileRef) -> BoxProviderFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move { self.0.read(file).await })
    }
}

impl ContentPublisher for StoredContent {
    fn volume(&self) -> &VolumeRef {
        self.0.volume()
    }

    fn stage<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            self.0
                .stage(operation, path, bytes, media_type, display_name)
                .await
        })
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
    let content = Arc::new(StoredContent(storage.clone()));
    let provider = DurableContextProvider::new(
        stream.clone(),
        StreamPath::new("context/continuing").map_err(|error| Error::Invalid(error.to_string()))?,
        "memory",
        "1",
        1,
        content.clone(),
        Limits {
            file_bytes: 65_536,
            render_bytes: 65_536,
            context_messages: 32,
            attachments: 32,
            path_bytes: 4_096,
            ..Limits::default()
        },
    )?
    .with_publisher(content)?;
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

fn typed_payload_source() -> Context {
    Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("instruction-secret-é🦀".repeat(3_500)),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall {
                    call_id: "previous-call".into(),
                    name: "echo".into(),
                    arguments: serde_json::json!({"value":"é🦀"}),
                }),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "previous-call".into(),
                    name: "echo".into(),
                    value: serde_json::json!({"value":"é🦀"}),
                }),
            },
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("current".into()),
            },
        ],
        current_input_index: Some(3),
        ..Context::default()
    }
}

#[tokio::test]
async fn large_context_payload_preserves_instruction_and_typed_tool_exchange() -> Result<()> {
    let stream: Arc<LostSessionAck> = Arc::new(LostSessionAck::default());
    let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 262_144).await?);
    let content = Arc::new(StoredContent(storage));
    let path = StreamPath::new("context/typed-payload")
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let limits = Limits {
        file_bytes: 262_144,
        render_bytes: 262_144,
        context_messages: 32,
        attachments: 32,
        path_bytes: 4_096,
        ..Limits::default()
    };
    let provider = DurableContextProvider::new(
        stream.clone(),
        path.clone(),
        "memory",
        "1",
        1,
        content.clone(),
        limits,
    )?
    .with_publisher(content.clone())?;
    let source = typed_payload_source();
    let record = provider
        .append(
            0,
            source.clone(),
            None,
            Bytes::from_static(b"typed-payload"),
        )
        .await?;
    assert!(record.content.descriptor().byte_length() > MAX_RECORD_BYTES as u64);
    let mut records = stream
        .read(ReadRequest {
            path: path.clone(),
            from: 0,
            limit: 1,
        })
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let stored = records
        .next()
        .await
        .ok_or_else(|| Error::Storage("missing stored revision".into()))?
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(stored.value.len() < MAX_RECORD_BYTES);
    let wire: serde_json::Value =
        serde_json::from_slice(&stored.value).map_err(|error| Error::Invalid(error.to_string()))?;
    let pointer: FileRef = serde_json::from_value(
        wire.get("context")
            .cloned()
            .ok_or_else(|| Error::Storage("missing payload pointer".into()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(pointer, record.content);
    assert!(
        !stored
            .value
            .windows(b"instruction-secret".len())
            .any(|bytes| bytes == b"instruction-secret")
    );
    let reader = DurableContextProvider::new(stream, path, "memory", "1", 1, content, limits)?;
    assert_eq!(reader.revision(1).await?.context, source);
    let prepared = PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new("synthetic", "typed-payload", "1", serde_json::json!({}))?,
            messages: reader.latest().await?.messages,
            tools: vec![ToolDefinition {
                name: "echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: serde_json::json!({"type":"object"}),
                output_schema: serde_json::json!({"type":"object"}),
            }],
            max_output_tokens: Some(1_024),
        },
        limits,
    )?;
    assert_eq!(prepared.request().messages, source.messages);
    Ok(())
}

#[tokio::test]
async fn checkpoint_envelope_binds_real_published_context_and_admitted_summary() -> Result<()> {
    let Fixture {
        storage,
        provider,
        source,
        compacted,
        reference,
        ..
    } = fixture().await?;
    let operation_id = reference
        .summary
        .as_ref()
        .ok_or_else(|| Error::Invalid("missing admitted summary".into()))?
        .operation_id;
    let records = provider
        .append_compaction(
            0,
            source.clone(),
            compacted.clone(),
            reference.clone(),
            Bytes::from_static(b"checkpoint-envelope-source"),
        )
        .await?;
    let proof_bytes = reference.encode()?;
    let proof = storage
        .stage(
            operation_id,
            "checkpoint-proof.json",
            &proof_bytes,
            "application/json",
            "checkpoint-proof.json",
        )
        .await?;
    // The canonical selection header is caller-owned here; this test qualifies payload
    // publication and admitted summary binding, not canonical coverage admission.
    let checkpoint = acyclic_harness::context::CanonicalContextCheckpoint {
        operation_id,
        selection: acyclic_harness::conversation::ModelContextSelection {
            conversation_revision: 3,
            message_ids: (0..3).map(|_| uuid::Uuid::new_v4()).collect(),
            checkpoint: None,
        },
        source: records[0].content.clone(),
        retained: records[1].content.clone(),
        compaction: proof,
    };
    checkpoint.validate_projection(&source, &compacted, &reference, Limits::default())?;
    assert!(
        checkpoint
            .validate_projection(&source, &source, &reference, Limits::default())
            .is_err()
    );
    let mut changed = checkpoint.clone();
    changed.operation_id = OperationId::new();
    assert!(
        changed
            .validate_projection(&source, &compacted, &reference, Limits::default())
            .is_err()
    );
    let encoded = checkpoint.encode(Limits::default())?;
    let pinned = storage
        .stage(
            operation_id,
            "checkpoint.json",
            &encoded,
            "application/json",
            "checkpoint.json",
        )
        .await?;
    let reader = StoredContent(storage.clone());
    let bytes = reader.read(&pinned).await?;
    pinned.descriptor().verify(&bytes)?;
    let reopened: acyclic_harness::context::CanonicalContextCheckpoint =
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(reopened, checkpoint);
    reopened.validate_projection(
        &records[0].context,
        &records[1].context,
        records[1]
            .compaction
            .as_ref()
            .ok_or_else(|| Error::Invalid("missing published proof".into()))?,
        Limits::default(),
    )?;
    Ok(())
}
