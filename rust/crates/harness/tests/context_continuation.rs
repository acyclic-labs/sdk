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
    counts: std::sync::atomic::AtomicUsize,
}

impl ModelProvider for SummaryModel {
    fn context_capacity(&self, _: &Model) -> Result<ModelContextCapacity> {
        Ok(ModelContextCapacity {
            context_tokens: 131_072,
            output_tokens: 4_096,
        })
    }

    fn count_tokens(&self, request: &PreparedModelRequest) -> Result<ModelTokenCount> {
        self.counts.fetch_add(1, Ordering::SeqCst);
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
                    content: acyclic_harness::model::ToolResultContent::Json {
                        value: serde_json::json!({"value":"é🦀"}),
                    },
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
                projection_schema: acyclic_harness::tool::json_projection_schema(
                    serde_json::json!({"type":"object"}),
                ),
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

struct InputDependentStage {
    revision: u32,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl acyclic_harness::context::ContextStage for InputDependentStage {
    fn name(&self) -> &str {
        "input-dependent"
    }
    fn contract(&self) -> serde_json::Value {
        serde_json::json!({ "name": self.name(), "revision": self.revision })
    }
    fn apply<'a>(
        &'a self,
        input: &'a acyclic_harness::context::ContextInput,
        mut context: Context,
        _: Limits,
    ) -> BoxProviderFuture<'a, Result<Context>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            context.messages.push(ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(format!(
                    "stage:{}:{}",
                    self.revision,
                    serde_json::to_string(&input.input)
                        .map_err(|error| Error::Invalid(error.to_string()))?
                )),
            });
            context.messages.reverse();
            let length = u32::try_from(context.messages.len())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            context.current_input_index =
                context.current_input_index.map(|index| length - 1 - index);
            Ok(context)
        })
    }
}

fn assert_fresh_stage_requests(provider: &SummaryModel, limits: Limits) -> usize {
    let requests = provider
        .requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let responses = requests
        .iter()
        .filter(|request| {
            request
                .request()
                .messages
                .iter()
                .any(|message| message.role == ModelRole::System)
        })
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), 10);
    for (turn, request) in responses.iter().enumerate() {
        let systems = request
            .request()
            .messages
            .iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>();
        assert_eq!(systems.len(), 1);
        let expected = if turn < 5 { "stage:1:" } else { "stage:2:" };
        assert!(
            matches!(systems.first().map(|message| &message.content), Some(ModelContent::Text(text)) if text.starts_with(expected))
        );
        assert!(request.request().messages.len() <= limits.context_messages);
    }
    assert!(requests.len() > responses.len());
    requests.len()
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one default-path history, reload, replay and publication qualification sequence"
)]
async fn default_canonical_continuation_keeps_stages_fresh_beyond_history_bound() -> Result<()> {
    let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 131_072).await?);
    let provider = Arc::new(SummaryModel::default());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let limits = Limits {
        context_messages: 8,
        ..Limits::default()
    };
    let model = Model::new("synthetic", "byte-counter", "1", serde_json::json!({}))?;
    let mut pipeline = ContextPipeline::new([Arc::new(InputDependentStage {
        revision: 1,
        calls: calls.clone(),
    })
        as Arc<dyn acyclic_harness::context::ContextStage>]);
    let build = |pipeline| {
        storage
            .builder()
            .model(model.clone(), provider.clone())
            .grant("model:generate")
            .context(pipeline)
            .limits(limits)
            .build()
    };
    let mut bundle = build(pipeline.clone())?;
    let mut operations = Vec::new();
    for turn in 0..10 {
        if turn == 5 {
            pipeline = pipeline.reload(ContextPipeline::new([Arc::new(InputDependentStage {
                revision: 2,
                calls: calls.clone(),
            })
                as Arc<dyn acyclic_harness::context::ContextStage>]))?;
            bundle = build(pipeline.clone())?;
        }
        let operation = OperationId::new();
        let file = storage
            .stage(
                operation,
                &format!("turns/{operation}/input.txt"),
                "q".repeat(60_000).as_bytes(),
                "text/plain",
                "input.txt",
            )
            .await?;
        let output = storage
            .run_conversation(&bundle, operation, file.clone(), Vec::new(), 1)
            .await?;
        operations.push((operation, file, output));
    }
    // Twenty canonical records exceed the eight-message model bound. This is a
    // live default-path fixture, not cold restoration or constant-memory proof.
    let generated = assert_fresh_stage_requests(&provider, limits);
    let stage_calls = calls.load(Ordering::SeqCst);
    // A turn from the current pinned pipeline, older than the latest retired
    // checkpoint, must hydrate its original selection instead of admitting a
    // fresh user or choosing newer context. No stage/model work is repeated.
    let (old_operation, old_file, old_output) = operations
        .get(6)
        .ok_or_else(|| Error::Invalid("no archived completed turn".into()))?;
    assert_eq!(
        &storage
            .run_conversation(&bundle, *old_operation, old_file.clone(), Vec::new(), 1)
            .await?,
        old_output
    );
    let (operation, file, expected) = operations
        .last()
        .ok_or_else(|| Error::Invalid("no completed default turn".into()))?;
    assert_eq!(
        &storage
            .run_conversation(&bundle, *operation, file.clone(), Vec::new(), 1)
            .await?,
        expected
    );
    assert_eq!(
        provider
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        generated
    );
    assert_eq!(calls.load(Ordering::SeqCst), stage_calls);

    let journal = storage.journal();
    let reference = acyclic_harness::executor::canonical_checkpoint_for_operation(
        journal.as_ref(),
        *operation,
        limits,
    )
    .await?
    .ok_or_else(|| Error::Invalid("default path did not publish checkpoint".into()))?;
    let (checkpoint, retained) =
        acyclic_harness::executor::load_canonical_checkpoint(journal.as_ref(), &reference, limits)
            .await?;
    let source: Context = serde_json::from_slice(&storage.read(&checkpoint.source).await?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(
        source
            .messages
            .iter()
            .all(|message| message.role != ModelRole::System)
    );
    assert!(
        retained
            .messages
            .iter()
            .all(|message| message.role != ModelRole::System)
    );
    assert_eq!(
        (
            checkpoint.selection.conversation_revision,
            checkpoint.selection.message_ids.len()
        ),
        (19, 2)
    );
    assert!(checkpoint.selection.checkpoint.is_some());
    assert_scoped_checkpoint_source(journal.as_ref(), &reference, limits, &retained).await?;
    assert_checkpoint_history_cut(&storage, journal.as_ref(), &reference, limits, &retained)
        .await?;
    assert_checkpoint_tail_source(
        storage.clone(),
        journal.as_ref(),
        &reference,
        limits,
        &retained,
    )
    .await?;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one actual checkpoint admission with owner, cursor and encoded-byte negative controls"
)]
async fn assert_checkpoint_history_cut(
    storage: &MemoryHarnessStorage,
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    expected: &Context,
) -> Result<()> {
    use acyclic_harness::store::HistoryReader;
    use acyclic_harness::{executor::load_canonical_checkpoint_at, runtime::RuntimeScope};

    let history = HistoryReader::new(storage.stream(), storage.conversation(), storage.verifier())?;
    let cursor = history.pin(0).await?;
    let scope = RuntimeScope::new(storage.owner_scope().capabilities().clone(), limits)?;
    let bytes = MAX_RECORD_BYTES as u64 * 2;
    let (checkpoint, retained) =
        load_canonical_checkpoint_at(journal, reference, limits, &scope, &history, &cursor, bytes)
            .await?;
    assert_eq!(&retained, expected);
    let foreign = MemoryHarnessStorage::new(AgentId::new(), 131_072).await?;
    assert!(matches!(
        load_canonical_checkpoint_at(
            foreign.journal().as_ref(),
            reference,
            limits,
            &scope,
            &history,
            &cursor,
            bytes
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    let unbound = LostCanonicalAck {
        inner: storage.journal(),
        fail: std::sync::atomic::AtomicBool::new(false),
        deny_reads: std::sync::atomic::AtomicBool::new(false),
        tamper: std::sync::atomic::AtomicUsize::new(0),
    };
    assert!(matches!(
        load_canonical_checkpoint_at(
            &unbound, reference, limits, &scope, &history, &cursor, bytes
        )
        .await,
        Err(Error::Unsupported(_))
    ));
    let (selection, _) = history
        .operation_event_bounded(checkpoint.operation_id, bytes)
        .await?;
    let selection = selection.ok_or_else(|| Error::Invalid("missing actual selection".into()))?;
    let mut cut = cursor.clone();
    cut.through_revision = selection.revision - 1;
    assert!(matches!(
        load_canonical_checkpoint_at(journal, reference, limits, &scope, &history, &cut, bytes)
            .await,
        Err(Error::Conflict(_))
    ));
    cut = cursor.clone();
    cut.after_revision = selection.revision;
    assert!(matches!(
        load_canonical_checkpoint_at(journal, reference, limits, &scope, &history, &cut, bytes)
            .await,
        Err(Error::Conflict(_))
    ));
    cut = cursor.clone();
    cut.authority.id = "foreign-checkpoint-owner".into();
    assert!(matches!(
        load_canonical_checkpoint_at(journal, reference, limits, &scope, &history, &cut, bytes)
            .await,
        Err(Error::Unauthorized(_))
    ));
    cut = cursor.clone();
    cut.through_revision += 1;
    assert!(matches!(
        load_canonical_checkpoint_at(journal, reference, limits, &scope, &history, &cut, bytes)
            .await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        load_canonical_checkpoint_at(journal, reference, limits, &scope, &history, &cursor, 0)
            .await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        load_canonical_checkpoint_at(journal, reference, limits, &scope, &history, &cursor, 1)
            .await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}

struct TailCheckpointRead<'a, P> {
    operation_id: OperationId,
    budget: acyclic_harness::store::HistoryReadLimits,
    journal: &'a dyn acyclic_harness::executor::ExecutionJournal,
    reference: &'a FileRef,
    limits: Limits,
    history: &'a acyclic_harness::store::HistoryReader<P>,
    cursor: &'a acyclic_harness::store::HistoryCursor,
    content: &'a StoredContent,
}

impl<P: StreamProvider> TailCheckpointRead<'_, P> {
    async fn assert_read_controls(
        &self,
        original: &acyclic_harness::runtime::RuntimeScope,
        retained: &Context,
        tail_message: &acyclic_harness::conversation::ConversationMessage,
        imported: &Context,
    ) -> Result<acyclic_harness::runtime::RuntimeScope> {
        use acyclic_harness::executor::load_canonical_checkpoint_through;
        use acyclic_harness::store::HistoryReadLimits;
        use acyclic_harness::{Capabilities, runtime::RuntimeScope};
        let Self {
            operation_id,
            budget,
            journal,
            reference,
            limits,
            history,
            cursor,
            content,
        } = *self;
        let (_, proof_bytes) = history
            .operation_event_bounded(operation_id, budget.maximum_bytes)
            .await?;
        for (through, allowance) in [
            (18, budget),
            (
                20,
                HistoryReadLimits {
                    maximum_events: 1,
                    ..budget
                },
            ),
            (
                20,
                HistoryReadLimits {
                    maximum_bytes: proof_bytes,
                    ..budget
                },
            ),
            (
                20,
                HistoryReadLimits {
                    maximum_events: 0,
                    ..budget
                },
            ),
        ] {
            assert!(matches!(
                load_canonical_checkpoint_through(
                    journal, reference, limits, original, history, cursor, through, allowance,
                    content,
                )
                .await,
                Err(Error::Invalid(_))
            ));
        }
        let grants = retained
            .messages
            .iter()
            .flat_map(|message| message.content.file_refs())
            .chain(retained.metadata.values())
            .map(FileRef::read_capability)
            .collect::<Result<Vec<_>>>()?;
        let denied = RuntimeScope::new(Capabilities::new(grants.clone()), limits)?;
        assert!(matches!(
            load_canonical_checkpoint_through(
                journal, reference, limits, &denied, history, cursor, 20, budget, content,
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        let exact = RuntimeScope::new(
            Capabilities::new(
                grants
                    .into_iter()
                    .chain([tail_message.content.read_capability()?]),
            ),
            limits,
        )?;
        assert_eq!(
            load_canonical_checkpoint_through(
                journal,
                reference,
                Limits::default(),
                &exact,
                history,
                cursor,
                20,
                budget,
                content,
            )
            .await?
            .1,
            *imported
        );
        Ok(exact)
    }
    async fn assert_later_cut_controls(
        &self,
        reducer: &acyclic_harness::core::Reducer,
        original: &acyclic_harness::runtime::RuntimeScope,
        exact: &acyclic_harness::runtime::RuntimeScope,
        imported: &Context,
        prepared_summary: &acyclic_harness::fork::PreparedSummaryFork,
    ) -> Result<()> {
        use acyclic_harness::executor::load_canonical_checkpoint_through;
        use acyclic_harness::store::HistoryReadLimits;
        let Self {
            journal,
            reference,
            limits,
            history,
            cursor,
            content,
            budget,
            ..
        } = *self;
        assert_eq!(
            load_canonical_checkpoint_through(
                journal, reference, limits, exact, history, cursor, 20, budget, content,
            )
            .await?
            .1,
            *imported
        );
        assert!(
            load_canonical_checkpoint_through(
                journal,
                reference,
                limits,
                original,
                history,
                cursor,
                21,
                HistoryReadLimits {
                    maximum_events: 3,
                    ..budget
                },
                content,
            )
            .await
            .is_err()
        );
        // A changed parent cannot silently relabel an old authenticated capture.
        assert!(matches!(
            acyclic_harness::fork::prepare_summary_fork_context(
                reducer,
                prepared_summary.selection().clone(),
                journal,
                original,
                history,
                cursor,
                content,
            )
            .await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(prepared_summary.through_sequence(), 20);
        assert_eq!(prepared_summary.context(), imported);
        let later = history.pin(0).await?;
        assert_eq!(
            load_canonical_checkpoint_through(
                journal,
                reference,
                limits,
                exact,
                history,
                &later,
                21,
                HistoryReadLimits {
                    maximum_events: 3,
                    ..budget
                },
                content,
            )
            .await?
            .1,
            *imported,
            "canonical interaction record consumes work but is not a model message"
        );
        assert!(matches!(
            load_canonical_checkpoint_through(
                journal, reference, limits, exact, history, &later, 21, budget, content,
            )
            .await,
            Err(Error::Invalid(_))
        ));
        Ok(())
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "actual published checkpoint plus exact tail, shared budgets, narrowed grants and ordinary source admission controls"
)]
async fn assert_checkpoint_tail_source(
    storage: Arc<MemoryHarnessStorage>,
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    retained: &Context,
) -> Result<()> {
    use acyclic_harness::{
        executor::load_canonical_checkpoint_through,
        runtime::RuntimeScope,
        store::{HistoryReadLimits, HistoryReader},
    };

    let history = HistoryReader::new(storage.stream(), storage.conversation(), storage.verifier())?;
    let cursor = history.pin(0).await?;
    let content = StoredContent(storage.clone());
    let budget = HistoryReadLimits {
        maximum_events: 2,
        maximum_bytes: MAX_RECORD_BYTES as u64 * 4,
    };
    let original = RuntimeScope::new(storage.owner_scope().capabilities().clone(), limits)?;
    let (checkpoint, imported) = load_canonical_checkpoint_through(
        journal, reference, limits, &original, &history, &cursor, 20, budget, &content,
    )
    .await?;
    assert_eq!(checkpoint.selection.conversation_revision, 19);
    assert_eq!(imported.current_input_index, None);
    assert_eq!(imported.messages.len(), retained.messages.len() + 1);
    assert_eq!(
        imported.messages.get(..retained.messages.len()),
        Some(retained.messages.as_slice())
    );
    let tail = history.conversation_range(&cursor, 19, 20, budget).await?;
    assert_eq!(tail.len(), 1);
    let tail_message = tail
        .first()
        .ok_or_else(|| Error::NotFound("actual canonical tail".into()))?;
    assert_eq!(
        tail_message.kind,
        acyclic_harness::conversation::MessageKind::Assistant
    );
    assert_eq!(
        imported.messages.last(),
        Some(&ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Parts(vec![ModelContentPart::File {
                file: tail_message.content.clone(),
                policy: FileProjectionPolicy::BoundedFull,
            }]),
        })
    );
    let read = TailCheckpointRead {
        operation_id: checkpoint.operation_id,
        budget,
        journal,
        reference,
        limits,
        history: &history,
        cursor: &cursor,
        content: &content,
    };
    let exact = read
        .assert_read_controls(&original, retained, tail_message, &imported)
        .await?;
    let model = Arc::new(SummaryModel::default());
    let executor = StockExecutor::new(
        Model::new(
            "synthetic",
            "checkpoint-and-tail",
            "1",
            serde_json::json!({}),
        )?,
        model.clone(),
        ContextPipeline::new([Arc::new(SourceStage::new(
            "published-summary-and-tail",
            "1",
            Arc::new(imported.clone()),
            ContextPlacement::Prepend,
        ))
            as Arc<dyn acyclic_harness::context::ContextStage>]),
        ToolRegistry::new(),
    )
    .with_limits(limits);
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("fresh input after exact summary and tail".into()),
        selected_context: None,
        max_steps: 1,
    };
    executor.execute(input.clone(), journal).await?;
    {
        let requests = model
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(requests.len(), 1);
        let messages = &requests
            .first()
            .ok_or_else(|| Error::Invalid("missing admitted request".into()))?
            .request()
            .messages;
        assert_eq!(
            messages.get(..imported.messages.len()),
            Some(imported.messages.as_slice())
        );
        assert_eq!(
            messages.last().map(|message| &message.content),
            Some(&input.input)
        );
    }
    // This is an original-owner import, not a child admission. A later canonical
    // append must not change either the original logical or event cutoff.
    let mut aggregate = acyclic_harness::store::StreamAggregate::open(
        storage.stream(),
        storage.conversation().clone(),
        storage.verifier(),
        acyclic_harness::core::SchemaRegistry::new(),
    )
    .await?
    .with_content_verifier(Arc::new(StoredContent(storage.clone())));
    let prepared_summary = acyclic_harness::fork::prepare_summary_fork_context(
        aggregate.reducer(),
        acyclic_harness::fork::SummaryForkSelection {
            checkpoint: reference.clone(),
            limits,
            history_limits: budget,
        },
        journal,
        &original,
        &history,
        &cursor,
        &content,
    )
    .await?;
    assert_eq!(prepared_summary.parent(), aggregate.reducer().authority());
    assert_eq!(prepared_summary.parent_revision(), cursor.through_revision);
    assert_eq!(prepared_summary.through_sequence(), 20);
    assert_eq!(prepared_summary.selection().checkpoint, *reference);
    assert_eq!(prepared_summary.checkpoint(), &checkpoint);
    assert_eq!(prepared_summary.context(), &imported);
    let id = uuid::Uuid::new_v4();
    aggregate
        .execute(acyclic_harness::core::Command {
            operation_id: OperationId::new(),
            idempotency_key: acyclic_harness::IdempotencyKey::new(format!(
                "checkpoint-tail-later:{id}"
            ))?,
            expected_revision: aggregate.reducer().revision(),
            scope: storage.owner_scope().clone(),
            causal_parent: None,
            action: acyclic_harness::core::Action::AppendConversationMessage {
                message: Box::new(acyclic_harness::conversation::ConversationMessage {
                    id,
                    sequence: 21,
                    kind: acyclic_harness::conversation::MessageKind::Interaction,
                    content: tail_message.content.clone(),
                    attachments: Vec::new().into(),
                    reply_to: None,
                    tool_call_id: None,
                    extensions: Default::default(),
                }),
            },
        })
        .await?;
    read.assert_later_cut_controls(
        aggregate.reducer(),
        &original,
        &exact,
        &imported,
        &prepared_summary,
    )
    .await?;
    Ok(())
}

async fn assert_scoped_checkpoint_source(
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    retained: &Context,
) -> Result<()> {
    use acyclic_harness::executor::load_canonical_checkpoint_for_scope;
    use acyclic_harness::{Capabilities, runtime::RuntimeScope};

    let grants = retained
        .messages
        .iter()
        .flat_map(|message| message.content.file_refs())
        .chain(retained.metadata.values())
        .map(FileRef::read_capability)
        .collect::<Result<Vec<_>>>()?;
    assert!(
        !grants.is_empty(),
        "published summary must retain file authority"
    );
    let scope = RuntimeScope::new(Capabilities::new(grants.clone()), limits)?;
    let (_, imported) =
        load_canonical_checkpoint_for_scope(journal, reference, Limits::default(), &scope).await?;
    assert_eq!(&imported, retained);
    let denied = scope.narrow(
        scope
            .grants()
            .without(&Capabilities::new([grants.first().cloned().ok_or_else(
                || Error::Invalid("missing retained read grant".into()),
            )?])),
        limits,
    )?;
    assert!(matches!(
        load_canonical_checkpoint_for_scope(journal, reference, Limits::default(), &denied).await,
        Err(Error::Unauthorized(_))
    ));
    let narrow = scope.narrow(
        scope.grants().clone(),
        Limits {
            render_bytes: 1,
            ..limits
        },
    )?;
    assert!(matches!(
        load_canonical_checkpoint_for_scope(journal, reference, Limits::default(), &narrow).await,
        Err(Error::Invalid(_))
    ));

    // Consume the actual default-published projection through the existing
    // source/stage path and admit an ordinary model operation with fresh input.
    let model = Arc::new(SummaryModel::default());
    let executor = StockExecutor::new(
        Model::new("synthetic", "checkpoint-import", "1", serde_json::json!({}))?,
        model.clone(),
        ContextPipeline::new([Arc::new(SourceStage::new(
            "published-summary",
            "1",
            Arc::new(imported),
            ContextPlacement::Prepend,
        ))
            as Arc<dyn acyclic_harness::context::ContextStage>]),
        ToolRegistry::new(),
    )
    .with_limits(limits);
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("new admission after summary import".into()),
        selected_context: None,
        max_steps: 1,
    };
    executor.execute(input.clone(), journal).await?;
    {
        let requests = model
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(requests.len(), 1);
        let messages = &requests
            .first()
            .ok_or_else(|| Error::Invalid("missing admitted request".into()))?
            .request()
            .messages;
        assert_eq!(
            messages.get(..retained.messages.len()),
            Some(retained.messages.as_slice())
        );
        assert_eq!(
            messages.last().map(|message| &message.content),
            Some(&input.input)
        );
    }
    let (_, unchanged) =
        load_canonical_checkpoint_for_scope(journal, reference, Limits::default(), &scope).await?;
    assert_eq!(&unchanged, retained);
    Ok(())
}

struct SnapshotReader {
    storage: Arc<MemoryHarnessStorage>,
    reads: std::sync::atomic::AtomicUsize,
    verifies: std::sync::atomic::AtomicUsize,
    missing: std::sync::atomic::AtomicBool,
}

impl ContentResidencyVerifier for SnapshotReader {
    fn verify<'a>(&'a self, file: &'a FileRef) -> BoxProviderFuture<'a, Result<()>> {
        self.verifies.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if self.missing.load(Ordering::SeqCst) {
                return Err(Error::NotFound("snapshot source unavailable".into()));
            }
            file.descriptor().verify(&self.storage.read(file).await?)
        })
    }

    fn read<'a>(&'a self, file: &'a FileRef) -> BoxProviderFuture<'a, Result<Vec<u8>>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if self.missing.load(Ordering::SeqCst) {
                return Err(Error::NotFound("snapshot source unavailable".into()));
            }
            self.storage.read(file).await
        })
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one authenticated snapshot through actual pipeline/model admission, metadata bounds, capture identity and retained replay"
)]
async fn pinned_snapshot_preserves_metadata_and_pins_capture_before_model_admission() -> Result<()>
{
    use acyclic_harness::context::{ContextInput, ContextStage as _, PinnedContextStage};
    use std::collections::BTreeMap;

    let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 131_072).await?);
    let metadata = storage
        .stage(
            OperationId::new(),
            "snapshot/state.txt",
            b"authoritative state",
            "text/plain",
            "state.txt",
        )
        .await?;
    let snapshot = Context {
        messages: vec![message(&storage, ModelRole::System, "pinned instruction").await?],
        metadata: BTreeMap::from([("snapshot.state".into(), metadata.clone())]),
        current_input_index: None,
    };
    let encoded = serde_json::to_vec(
        &serde_json::to_value(&snapshot).map_err(|error| Error::Invalid(error.to_string()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let reference = storage
        .stage(
            OperationId::new(),
            "snapshot/context.json",
            &encoded,
            "application/json",
            "context.json",
        )
        .await?;
    let reader = Arc::new(SnapshotReader {
        storage: storage.clone(),
        reads: std::sync::atomic::AtomicUsize::new(0),
        verifies: std::sync::atomic::AtomicUsize::new(0),
        missing: std::sync::atomic::AtomicBool::new(false),
    });
    let limits = Limits {
        context_messages: 8,
        attachments: 4,
        ..Limits::default()
    };
    let stage = Arc::new(
        PinnedContextStage::capture(
            "pinned-snapshot",
            "1",
            reference.clone(),
            reader.clone(),
            ContextPlacement::Prepend,
            limits,
        )
        .await?,
    );
    assert_eq!(
        stage.contract().get("reference"),
        Some(&serde_json::to_value(&reference).map_err(|error| Error::Invalid(error.to_string()))?)
    );
    let input = ContextInput {
        input: ModelContent::Text("fresh input".into()),
        selected_context: None,
        step: 0,
        prior_messages: Vec::new(),
    };
    let pipeline =
        ContextPipeline::new([stage.clone() as Arc<dyn acyclic_harness::context::ContextStage>]);
    let composed = pipeline.run_bounded(&input, limits).await?;
    assert_eq!(composed.metadata, snapshot.metadata);
    assert_eq!(composed.current_input_index, Some(1));
    assert_eq!(composed.messages.first(), snapshot.messages.first());
    assert_eq!(
        composed.messages.last().map(|message| &message.content),
        Some(&input.input)
    );
    let verifies = reader.verifies.load(Ordering::SeqCst);
    let mut collision = ContextPipeline::base_context(&input, limits)?;
    collision.metadata = snapshot.metadata.clone();
    assert!(matches!(
        stage.apply(&input, collision, limits).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(reader.verifies.load(Ordering::SeqCst), verifies);
    let reads = reader.reads.load(Ordering::SeqCst);
    assert!(matches!(
        PinnedContextStage::capture(
            "pinned-snapshot",
            "1",
            reference.clone(),
            reader.clone(),
            ContextPlacement::Prepend,
            Limits {
                render_bytes: 1,
                ..limits
            },
        )
        .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(reader.reads.load(Ordering::SeqCst), reads);
    assert!(matches!(
        pipeline
            .run_bounded(
                &input,
                Limits {
                    context_messages: 1,
                    ..limits
                }
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(reader.verifies.load(Ordering::SeqCst), verifies);

    // The retained capture itself must fit the narrowed receiving admission,
    // even when every message/metadata payload still fits independently.
    let narrow_file_bytes = reference.descriptor().byte_length() - 1;
    assert!(
        snapshot
            .messages
            .iter()
            .flat_map(|message| message.content.file_refs())
            .chain(snapshot.metadata.values())
            .all(|file| file.descriptor().byte_length() <= narrow_file_bytes)
    );
    let counts_before_narrow = (
        reader.reads.load(Ordering::SeqCst),
        reader.verifies.load(Ordering::SeqCst),
    );
    assert!(matches!(
        pipeline
            .run_bounded(
                &input,
                Limits {
                    file_bytes: narrow_file_bytes,
                    ..limits
                }
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        (
            reader.reads.load(Ordering::SeqCst),
            reader.verifies.load(Ordering::SeqCst)
        ),
        counts_before_narrow
    );

    let delta = Context {
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: input.input.clone(),
        }],
        metadata: BTreeMap::from([("fresh.state".into(), metadata.clone())]),
        current_input_index: Some(0),
    };
    let continued = ContextPipeline::continue_base(snapshot.clone(), delta.clone(), limits)?;
    assert_eq!(continued.metadata.len(), 2);
    assert_eq!(continued.metadata.get("snapshot.state"), Some(&metadata));
    assert_eq!(continued.metadata.get("fresh.state"), Some(&metadata));
    assert_eq!(continued.current_input_index, Some(1));
    assert!(matches!(
        ContextPipeline::continue_base(
            snapshot.clone(),
            delta.clone(),
            Limits {
                attachments: 1,
                ..limits
            },
        ),
        Err(Error::Invalid(_))
    ));
    let combined_bytes = serde_json::to_vec(&continued)
        .map_err(|error| Error::Invalid(error.to_string()))?
        .len() as u64;
    assert!(
        (serde_json::to_vec(&snapshot)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .len() as u64)
            < combined_bytes - 1
    );
    assert!(
        (serde_json::to_vec(&delta)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .len() as u64)
            < combined_bytes - 1
    );
    assert!(matches!(
        ContextPipeline::continue_base(
            snapshot.clone(),
            delta.clone(),
            Limits {
                render_bytes: combined_bytes - 1,
                ..limits
            },
        ),
        Err(Error::Invalid(_))
    ));
    let mut duplicate = delta;
    duplicate.metadata = snapshot.metadata.clone();
    assert!(matches!(
        ContextPipeline::continue_base(snapshot.clone(), duplicate, limits),
        Err(Error::Conflict(_))
    ));

    let model = Arc::new(SummaryModel::default());
    let snapshot_model = Model::new(
        "synthetic",
        "snapshot-admission",
        "1",
        serde_json::json!({}),
    )?;
    let build = |pipeline| {
        StockExecutor::new(
            snapshot_model.clone(),
            model.clone(),
            pipeline,
            ToolRegistry::new(),
        )
        .with_limits(limits)
        .with_compaction_policy(CompactionPolicy::Disabled)
    };
    let executor = build(pipeline);
    let turn = TurnInput {
        operation_id: OperationId::new(),
        input: input.input.clone(),
        selected_context: None,
        max_steps: 1,
    };
    let journal = storage.journal();
    let output = executor.execute(turn.clone(), journal.as_ref()).await?;
    let records = journal.replay(turn.operation_id, 0, 32).await?;
    let projection = records
        .iter()
        .find_map(|record| match &record.event {
            acyclic_harness::executor::ExecutionEvent::ContextPrepared {
                step: 0,
                projection,
                ..
            } => Some(projection),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("actual admitted snapshot projection".into()))?;
    let projection_bytes = journal.load(projection).await?;
    projection.descriptor().verify(&projection_bytes)?;
    let projection_wire: serde_json::Value = serde_json::from_slice(&projection_bytes)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let prepared: Context = serde_json::from_value(
        projection_wire
            .get("context")
            .cloned()
            .ok_or_else(|| Error::Invalid("prepared projection has no context".into()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(prepared, composed);
    {
        let requests = model
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests.first().map(|request| &request.request().messages),
            Some(&composed.messages)
        );
    }
    reader.missing.store(true, Ordering::SeqCst);
    let counts = (
        reader.reads.load(Ordering::SeqCst),
        reader.verifies.load(Ordering::SeqCst),
    );
    assert_eq!(
        executor.execute(turn.clone(), journal.as_ref()).await?,
        output
    );
    assert_eq!(
        (
            reader.reads.load(Ordering::SeqCst),
            reader.verifies.load(Ordering::SeqCst)
        ),
        counts
    );
    assert!(matches!(
        executor
            .execute(
                TurnInput {
                    operation_id: OperationId::new(),
                    ..turn.clone()
                },
                journal.as_ref()
            )
            .await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        model
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        1
    );
    reader.missing.store(false, Ordering::SeqCst);
    let changed = storage
        .stage(
            OperationId::new(),
            "snapshot/other-context.json",
            &encoded,
            "application/json",
            "context.json",
        )
        .await?;
    let other_stage = PinnedContextStage::capture(
        "pinned-snapshot",
        "1",
        changed,
        reader,
        ContextPlacement::Prepend,
        limits,
    )
    .await?;
    assert_ne!(stage.contract(), other_stage.contract());
    let changed_executor = build(ContextPipeline::new([
        Arc::new(other_stage) as Arc<dyn acyclic_harness::context::ContextStage>
    ]));
    assert!(matches!(
        changed_executor.execute(turn, journal.as_ref()).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        model
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        1
    );
    Ok(())
}

struct LostCanonicalAck {
    inner: Arc<dyn acyclic_harness::executor::ExecutionJournal>,
    fail: std::sync::atomic::AtomicBool,
    deny_reads: std::sync::atomic::AtomicBool,
    tamper: std::sync::atomic::AtomicUsize,
}

impl acyclic_harness::executor::ExecutionJournal for LostCanonicalAck {
    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
    ) -> BoxProviderFuture<'a, Result<Vec<acyclic_harness::executor::ExecutionRecord>>> {
        self.inner.replay(operation, after, maximum)
    }
    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: String,
        event: acyclic_harness::executor::ExecutionEvent,
    ) -> BoxProviderFuture<'a, Result<()>> {
        self.inner.append(operation, key, event)
    }
    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        tail: u64,
        key: String,
        event: acyclic_harness::executor::ExecutionEvent,
    ) -> BoxProviderFuture<'a, Result<bool>> {
        Box::pin(async move {
            let compacted = matches!(
                event,
                acyclic_harness::executor::ExecutionEvent::ContextCompacted { step: 0, .. }
            );
            let mode = if compacted {
                self.tamper.swap(0, Ordering::SeqCst)
            } else {
                0
            };
            let event = if mode == 0 {
                event
            } else {
                fabricate_checkpoint(self.inner.as_ref(), operation, event, mode).await?
            };
            let committed = self
                .inner
                .append_if_tail(operation, tail, key, event)
                .await?;
            if committed && compacted && self.fail.swap(false, Ordering::SeqCst) {
                Err(Error::Storage(
                    "lost canonical compaction acknowledgement".into(),
                ))
            } else {
                Ok(committed)
            }
        })
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: String,
        bytes: Vec<u8>,
        media_type: &'static str,
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        self.inner.stage(operation, key, bytes, media_type)
    }
    fn load<'a>(&'a self, file: &'a FileRef) -> BoxProviderFuture<'a, Result<Vec<u8>>> {
        self.inner.load(file)
    }
    fn verify_input_file<'a>(&'a self, file: &'a FileRef) -> BoxProviderFuture<'a, Result<()>> {
        if self.deny_reads.load(Ordering::SeqCst) {
            Box::pin(async { Err(Error::Unauthorized("retained payload read denied".into())) })
        } else {
            self.inner.verify_input_file(file)
        }
    }
    fn verify_selected_context<'a>(
        &'a self,
        operation: OperationId,
        selected: &'a acyclic_harness::projection::SelectedModelContext,
    ) -> BoxProviderFuture<'a, Result<()>> {
        self.inner.verify_selected_context(operation, selected)
    }
    fn open_interaction<'a>(
        &'a self,
        id: acyclic_harness::InteractionId,
        interaction: acyclic_harness::interaction::Interaction,
    ) -> BoxProviderFuture<'a, Result<()>> {
        self.inner.open_interaction(id, interaction)
    }
    fn interaction_outcome<'a>(
        &'a self,
        id: acyclic_harness::InteractionId,
    ) -> BoxProviderFuture<'a, Result<Option<acyclic_harness::interaction::InteractionOutcome>>>
    {
        self.inner.interaction_outcome(id)
    }
}

#[tokio::test]
async fn default_canonical_checkpoint_recovers_lost_commit_ack_without_recount_or_stage_replay()
-> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 131_072).await?;
    let provider = Arc::new(SummaryModel::default());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let journal = Arc::new(LostCanonicalAck {
        inner: storage.journal(),
        fail: std::sync::atomic::AtomicBool::new(true),
        deny_reads: std::sync::atomic::AtomicBool::new(false),
        tamper: std::sync::atomic::AtomicUsize::new(0),
    });
    let bundle = storage
        .builder()
        .journal(journal.clone())
        .grant("model:generate")
        .model(
            Model::new("synthetic", "byte-counter", "1", serde_json::json!({}))?,
            provider.clone(),
        )
        .context(ContextPipeline::new([Arc::new(InputDependentStage {
            revision: 1,
            calls: calls.clone(),
        })
            as Arc<dyn acyclic_harness::context::ContextStage>]))
        .build()?;
    storage.run_prompt(&bundle, &"q".repeat(60_000)).await?;
    let operation = OperationId::new();
    let file = storage
        .stage(
            operation,
            "fault/current.txt",
            "q".repeat(60_000).as_bytes(),
            "text/plain",
            "current.txt",
        )
        .await?;
    let error = storage
        .run_conversation(&bundle, operation, file.clone(), Vec::new(), 1)
        .await;
    assert!(matches!(error, Err(Error::Storage(message)) if message.contains("lost canonical")));
    assert!(
        acyclic_harness::executor::canonical_checkpoint_for_operation(
            storage.journal().as_ref(),
            operation,
            Limits::default()
        )
        .await?
        .is_some()
    );
    let generated = provider
        .requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .len();
    let counted = provider.counts.load(Ordering::SeqCst);
    let transformed = calls.load(Ordering::SeqCst);
    assert!(storage.run_prompt(&bundle, "another turn").await.is_err());
    let output = storage
        .run_conversation(&bundle, operation, file, Vec::new(), 1)
        .await?;
    assert_eq!(output.text, "retained summary");
    assert_eq!(
        provider
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        generated + 1
    );
    assert_eq!(provider.counts.load(Ordering::SeqCst), counted);
    assert_eq!(calls.load(Ordering::SeqCst), transformed);
    let records = storage.journal().replay(operation, 0, 64).await?;
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record.event,
                acyclic_harness::executor::ExecutionEvent::ContextCompacted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record.event,
                acyclic_harness::executor::ExecutionEvent::ModelStarted {
                    purpose: acyclic_harness::executor::ModelPurpose::Summary,
                    ..
                }
            ))
            .count(),
        1
    );
    storage.run_prompt(&bundle, "after recovery").await?;
    assert_checkpoint_read_admission(&storage, &journal, operation).await?;
    Ok(())
}

async fn assert_checkpoint_read_admission(
    storage: &MemoryHarnessStorage,
    journal: &LostCanonicalAck,
    operation: OperationId,
) -> Result<()> {
    let limits = Limits::default();
    let reference =
        acyclic_harness::executor::canonical_checkpoint_for_operation(journal, operation, limits)
            .await?
            .ok_or_else(|| Error::Invalid("recovered checkpoint missing".into()))?;
    let (mut envelope, _) =
        acyclic_harness::executor::load_canonical_checkpoint(journal, &reference, limits).await?;
    journal.deny_reads.store(true, Ordering::SeqCst);
    assert!(
        matches!(acyclic_harness::executor::load_canonical_checkpoint(journal, &reference, limits).await,
        Err(Error::Unauthorized(message)) if message.contains("retained payload"))
    );
    journal.deny_reads.store(false, Ordering::SeqCst);
    envelope.operation_id = OperationId::new();
    let unpublished = storage
        .stage(
            envelope.operation_id,
            "fault/unpublished-checkpoint.json",
            &envelope.encode(limits)?,
            "application/json",
            "checkpoint.json",
        )
        .await?;
    assert!(
        matches!(acyclic_harness::executor::load_canonical_checkpoint(journal, &unpublished, limits).await,
        Err(Error::Conflict(message)) if message.contains("not published"))
    );
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one real-journal admission, retry and operation-isolation timeline"
)]
async fn local_journal_admits_compaction_only_after_one_settled_summary() -> Result<()> {
    use acyclic_harness::executor::{ExecutionEvent, ModelPurpose};
    let storage = MemoryHarnessStorage::new(AgentId::new(), 131_072).await?;
    let journal = storage.journal();
    let operation = OperationId::new();
    let context = journal
        .stage(
            operation,
            "temporal-context".into(),
            b"{\"canonical\":null,\"checkpoint\":null,\"context\":{\"current_input_index\":0,\"messages\":[{\"content\":{\"kind\":\"text\",\"text\":\"source\"},\"role\":\"user\"}],\"metadata\":{}}}".to_vec(),
            "application/json",
        )
        .await?;
    let compacted = ExecutionEvent::ContextCompacted {
        step: 0,
        projection: context.clone(),
        compaction: context.clone(),
        accounting: context.clone(),
    };
    assert!(
        journal
            .append_if_tail(operation, 0, "premature".into(), compacted.clone())
            .await
            .is_err()
    );
    journal
        .append(
            operation,
            "started".into(),
            ExecutionEvent::Started {
                request_digest: [1; 32],
            },
        )
        .await?;
    journal
        .append(
            operation,
            "prepared".into(),
            ExecutionEvent::ContextPrepared {
                step: 0,
                projection: context.clone(),
                accounting: None,
            },
        )
        .await?;
    assert!(
        journal
            .append(operation, "no-summary".into(), compacted.clone())
            .await
            .is_err()
    );
    let request = PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new("synthetic", "journal-test", "1", serde_json::json!({}))?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("source".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: Some(32),
        },
        Limits::default(),
    )?;
    let request_ref = journal
        .stage(
            operation,
            "summary-request".into(),
            request.bytes().to_vec(),
            "application/json",
        )
        .await?;
    journal
        .append(
            operation,
            "summary-started".into(),
            ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Summary,
                request_digest: request.manifest().request_digest,
                request: request_ref,
            },
        )
        .await?;
    assert!(
        journal
            .append(operation, "unsettled-summary".into(), compacted.clone())
            .await
            .is_err()
    );
    let completed = journal
        .stage(
            operation,
            "summary-completed".into(),
            b"{\"kind\":\"completed\",\"metadata\":{}}".to_vec(),
            "application/json",
        )
        .await?;
    journal
        .append(
            operation,
            "summary-completed".into(),
            ExecutionEvent::Model {
                step: 0,
                purpose: ModelPurpose::Summary,
                event: completed,
            },
        )
        .await?;
    assert!(
        !journal
            .append_if_tail(operation, 3, "stale-tail".into(), compacted.clone())
            .await?
    );
    assert!(
        journal
            .append_if_tail(operation, 4, "compacted".into(), compacted.clone())
            .await?
    );
    assert!(
        journal
            .append_if_tail(operation, 4, "compacted".into(), compacted.clone())
            .await?
    );
    assert!(
        journal
            .append(operation, "duplicate-compacted".into(), compacted)
            .await
            .is_err()
    );
    // A different operation must start at its own empty timeline; switching the
    // rebuildable active index must not borrow the first operation's admission.
    let other = OperationId::new();
    journal
        .append(
            other,
            "started".into(),
            ExecutionEvent::Started {
                request_digest: [2; 32],
            },
        )
        .await?;
    assert_eq!(journal.replay(operation, 0, 64).await?.len(), 5);
    assert_eq!(journal.replay(other, 0, 64).await?.len(), 1);
    assert!(
        journal
            .append_if_tail(
                operation,
                4,
                "compacted".into(),
                journal
                    .replay(operation, 4, 1)
                    .await?
                    .first()
                    .ok_or_else(|| Error::Invalid("compaction record missing".into()))?
                    .event
                    .clone()
            )
            .await?
    );
    Ok(())
}

async fn stage_fixture_json<T: serde::Serialize>(
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    operation: OperationId,
    key: &str,
    value: &T,
) -> Result<FileRef> {
    // Value's sorted object map uses the same canonical order as the wire writer.
    let value = serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    let bytes = serde_json::to_vec(&value).map_err(|error| Error::Invalid(error.to_string()))?;
    journal
        .stage(operation, key.into(), bytes, "application/json")
        .await
}

async fn fabricate_checkpoint(
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    operation: OperationId,
    event: acyclic_harness::executor::ExecutionEvent,
    mode: usize,
) -> Result<acyclic_harness::executor::ExecutionEvent> {
    use acyclic_harness::context::CanonicalContextCheckpoint;
    use acyclic_harness::executor::ExecutionEvent;
    let ExecutionEvent::ContextCompacted {
        step,
        projection,
        compaction,
        accounting,
    } = event
    else {
        return Err(Error::Invalid("fixture requires compacted context".into()));
    };
    let decode = |bytes: &[u8]| {
        serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))
    };
    let mut wrapper: serde_json::Value = decode(&journal.load(&projection).await?)?;
    let checkpoint: FileRef = serde_json::from_value(
        wrapper
            .get("checkpoint")
            .cloned()
            .ok_or_else(|| Error::Invalid("fixture checkpoint missing".into()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut envelope: CanonicalContextCheckpoint =
        serde_json::from_slice(&journal.load(&checkpoint).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut source: Context = serde_json::from_slice(&journal.load(&envelope.source).await?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let proof: CompactionReference = serde_json::from_slice(&journal.load(&compaction).await?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut summary = proof
        .summary
        .ok_or_else(|| Error::Invalid("fixture summary missing".into()))?;
    if mode == 1 {
        source
            .messages
            .last_mut()
            .ok_or_else(|| Error::Invalid("fixture source empty".into()))?
            .content = ModelContent::Text("fabricated current".into());
    } else {
        summary.output = journal
            .stage(
                operation,
                "poison-summary".into(),
                b"fabricated summary".to_vec(),
                "text/plain",
            )
            .await?;
    }
    let (retained, proof) = DurableContextProvider::compact(
        &source,
        proof.maximum_messages as usize,
        Some(summary),
        proof.retention,
    )?;
    if mode == 1 {
        envelope.source = stage_fixture_json(journal, operation, "poison-source", &source).await?;
    }
    envelope.retained =
        stage_fixture_json(journal, operation, "poison-retained", &retained).await?;
    envelope.compaction = journal
        .stage(
            operation,
            "poison-proof".into(),
            proof.encode()?,
            "application/json",
        )
        .await?;
    // The substituted payloads satisfy the public envelope and compaction proof.
    // Only owning-history/model-observation provenance can reject them.
    envelope.validate_projection(&source, &retained, &proof, Limits::default())?;
    let checkpoint = journal
        .stage(
            operation,
            "poison-checkpoint".into(),
            envelope.encode(Limits::default())?,
            "application/json",
        )
        .await?;
    *wrapper
        .get_mut("canonical")
        .ok_or_else(|| Error::Invalid("fixture canonical missing".into()))? =
        serde_json::to_value(&envelope.source)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    *wrapper
        .get_mut("checkpoint")
        .ok_or_else(|| Error::Invalid("fixture checkpoint missing".into()))? =
        serde_json::to_value(checkpoint).map_err(|error| Error::Invalid(error.to_string()))?;
    let projection = stage_fixture_json(journal, operation, "poison-projection", &wrapper).await?;
    Ok(ExecutionEvent::ContextCompacted {
        step,
        projection,
        compaction: envelope.compaction,
        accounting,
    })
}

#[tokio::test]
async fn canonical_publication_rejects_fabricated_source_and_summary_then_recovers() -> Result<()> {
    use acyclic_harness::executor::{ExecutionEvent, ModelPurpose};
    for mode in [1, 2] {
        let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 131_072).await?);
        let provider = Arc::new(SummaryModel::default());
        let journal = Arc::new(LostCanonicalAck {
            inner: storage.journal(),
            fail: std::sync::atomic::AtomicBool::new(false),
            deny_reads: std::sync::atomic::AtomicBool::new(false),
            tamper: std::sync::atomic::AtomicUsize::new(mode),
        });
        let bundle = storage
            .builder()
            .journal(journal)
            .grant("model:generate")
            .model(
                Model::new("synthetic", "byte-counter", "1", serde_json::json!({}))?,
                provider.clone(),
            )
            .build()?;
        storage.run_prompt(&bundle, &"q".repeat(60_000)).await?;
        let operation = OperationId::new();
        let file = storage
            .stage(
                operation,
                "fault/current.txt",
                "q".repeat(60_000).as_bytes(),
                "text/plain",
                "current.txt",
            )
            .await?;
        let error = storage
            .run_conversation(&bundle, operation, file.clone(), Vec::new(), 1)
            .await;
        let expected = if mode == 1 {
            "canonical base and delta"
        } else {
            "output differs from admitted"
        };
        assert!(matches!(error, Err(Error::Conflict(message)) if message.contains(expected)));
        let records = storage.journal().replay(operation, 0, 64).await?;
        assert!(
            !records
                .iter()
                .any(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
        );
        let output = storage
            .run_conversation(&bundle, operation, file, Vec::new(), 1)
            .await?;
        assert_eq!(output.text, "retained summary");
        let records = storage.journal().replay(operation, 0, 64).await?;
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    record.event,
                    ExecutionEvent::ModelStarted {
                        purpose: ModelPurpose::Summary,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
                .count(),
            1
        );
        assert_eq!(
            provider
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            3
        );
        let previous_reference = acyclic_harness::executor::canonical_checkpoint_for_operation(
            storage.journal().as_ref(),
            operation,
            Limits::default(),
        )
        .await?
        .ok_or_else(|| Error::NotFound("previous published checkpoint".into()))?;
        let (_, previous_retained) = acyclic_harness::executor::load_canonical_checkpoint(
            storage.journal().as_ref(),
            &previous_reference,
            Limits::default(),
        )
        .await?;
        assert_checkpoint_metadata_continuation(
            &storage,
            &bundle,
            &previous_reference,
            &previous_retained,
        )
        .await?;
    }
    Ok(())
}

async fn append_private_checkpoint_metadata(
    storage: &Arc<MemoryHarnessStorage>,
) -> Result<(FileRef, Vec<uuid::Uuid>)> {
    // Canonical metadata between model messages must be read and verified,
    // but must not become provider-visible instruction or turn content.
    let metadata = storage
        .stage(
            OperationId::new(),
            "fault/metadata.txt",
            b"private timeline metadata",
            "text/plain",
            "metadata.txt",
        )
        .await?;
    let mut history = acyclic_harness::store::StreamAggregate::open(
        storage.stream(),
        storage.conversation().clone(),
        storage.verifier(),
        acyclic_harness::core::SchemaRegistry::new(),
    )
    .await?
    .with_content_verifier(Arc::new(StoredContent(storage.clone())));
    let mut metadata_ids = Vec::new();
    for kind in [
        acyclic_harness::conversation::MessageKind::Interaction,
        acyclic_harness::conversation::MessageKind::Permission,
    ] {
        let sequence = history
            .reducer()
            .conversation()
            .ok_or_else(|| Error::NotFound("canonical conversation".into()))?
            .messages()
            .len() as u64
            + 1;
        let id = uuid::Uuid::new_v4();
        metadata_ids.push(id);
        history
            .execute(acyclic_harness::core::Command {
                operation_id: OperationId::new(),
                idempotency_key: acyclic_harness::IdempotencyKey::new(format!(
                    "cold-metadata:{id}"
                ))?,
                expected_revision: history.reducer().revision(),
                scope: storage.owner_scope().clone(),
                causal_parent: None,
                action: acyclic_harness::core::Action::AppendConversationMessage {
                    message: Box::new(acyclic_harness::conversation::ConversationMessage {
                        id,
                        sequence,
                        kind,
                        content: metadata.clone(),
                        attachments: Vec::new().into(),
                        reply_to: None,
                        tool_call_id: None,
                        extensions: Default::default(),
                    }),
                },
            })
            .await?;
    }
    Ok((metadata, metadata_ids))
}

async fn assert_checkpoint_metadata_continuation(
    storage: &Arc<MemoryHarnessStorage>,
    bundle: &acyclic_harness::Harness,
    previous_reference: &FileRef,
    previous_retained: &Context,
) -> Result<()> {
    use acyclic_harness::executor::ExecutionEvent;
    let (metadata, metadata_ids) = append_private_checkpoint_metadata(storage).await?;
    let next_operation = OperationId::new();
    let next_file = storage
        .stage(
            next_operation,
            "fault/next.txt",
            "q".repeat(60_000).as_bytes(),
            "text/plain",
            "next.txt",
        )
        .await?;
    let next_output = storage
        .run_conversation(bundle, next_operation, next_file, Vec::new(), 1)
        .await?;
    assert_eq!(next_output.text, "retained summary");
    let next_reference = acyclic_harness::executor::canonical_checkpoint_for_operation(
        storage.journal().as_ref(),
        next_operation,
        Limits::default(),
    )
    .await?
    .ok_or_else(|| Error::NotFound("next published checkpoint".into()))?;
    let (next_envelope, _) = acyclic_harness::executor::load_canonical_checkpoint(
        storage.journal().as_ref(),
        &next_reference,
        Limits::default(),
    )
    .await?;
    assert_eq!(
        next_envelope.selection.checkpoint.as_ref(),
        Some(previous_reference)
    );
    assert!(
        metadata_ids
            .iter()
            .all(|id| !next_envelope.selection.message_ids.contains(id))
    );
    let source: Context =
        serde_json::from_slice(&storage.journal().load(&next_envelope.source).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        source.messages.get(..previous_retained.messages.len()),
        Some(previous_retained.messages.as_slice())
    );
    assert!(
        source
            .messages
            .iter()
            .flat_map(|message| message.content.file_refs())
            .all(|file| file != &metadata)
    );
    assert_eq!(
        storage
            .journal()
            .replay(next_operation, 0, 64)
            .await?
            .iter()
            .filter(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }))
            .count(),
        1
    );
    Ok(())
}
