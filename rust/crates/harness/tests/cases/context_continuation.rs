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
    FileProjectionPolicy, Model, ModelContent, ModelContentPart, ModelEvent, ModelMessage,
    ModelRequest, ModelRole, PreparedModelRequest,
};
use acyclic_harness::test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn};
use acyclic_harness::tool::{ToolDefinition, ToolRegistry};
use acyclic_harness::{AgentId, Error, OperationId, Result};
use acyclic_stream::{MAX_RECORD_BYTES, ReadRequest, StreamPath, StreamProvider};
use bytes::Bytes;
use futures::StreamExt as _;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[path = "../support/stream/mod.rs"]
mod stream_fixture;

use crate::context_continuation::stream_fixture as stream_fault;
use stream_fault::LostSessionAck;

struct StoredContent(Arc<MemoryHarnessStorage>);

fn summary_model(turns: usize) -> (Arc<ScriptedModelProvider>, Arc<AtomicUsize>) {
    let counts = Arc::new(AtomicUsize::new(0));
    let counter = counts.clone();
    let provider = ScriptedModelProvider::new((0..turns).map(|_| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::Content {
                delta: "retained summary".into(),
            }),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ])
    }))
    .with_context_capacity(ModelContextCapacity {
        context_tokens: 131_072,
        output_tokens: Some(4_096),
    })
    .with_token_counter(move |request| {
        counter.fetch_add(1, Ordering::SeqCst);
        let message_tokens = request
            .request()
            .messages
            .iter()
            .map(|message| {
                let encoded = serde_json::to_vec(message)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let bytes =
                    message
                        .content
                        .file_refs()
                        .try_fold(encoded.len() as u64, |total, file| {
                            total
                                .checked_add(file.descriptor().byte_length())
                                .ok_or_else(|| Error::Invalid("synthetic count overflow".into()))
                        })?;
                u32::try_from(bytes)
                    .map_err(|_| Error::Invalid("synthetic count exceeds u32".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ModelTokenCount::from_upper_bounds(
            request.request_digest(),
            512,
            message_tokens,
        ))
    });
    (Arc::new(provider), counts)
}

impl ContentResidencyVerifier for StoredContent {
    fn verify<'a>(
        &'a self,
        file: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            if file.descriptor().byte_length() > maximum_bytes {
                return Err(Error::Invalid("content exceeds selected limit".into()));
            }
            file.descriptor()
                .verify(&self.0.read(file).await?, &mut None)
        })
    }

    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>
    {
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            if file.descriptor().byte_length() > maximum_bytes {
                return Err(Error::Invalid("content exceeds selected limit".into()));
            }
            self.0.read(file).await
        })
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
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<
        acyclic_harness::conversation::BoundedContentFuture<
            'a,
            (
                FileRef,
                Option<acyclic_harness::conversation::ContentHold<'a>>,
            ),
        >,
    > {
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            self.0
                .stage(operation, path, bytes, media_type, display_name)
                .await
                .map(|file| (file, None))
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
        Model::new(
            "synthetic".into(),
            "summary".into(),
            "1".into(),
            serde_json::json!({}),
        )?,
        summary_model(1).0,
        ContextPipeline::default(),
        ToolRegistry::default(),
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
    let (compacted, reference) = DurableContextProvider::compact(
        &source,
        2,
        Some(summary),
        &CompactionRetention::default(),
        None,
    )?;
    Ok(Fixture {
        stream,
        storage,
        provider,
        source,
        compacted,
        reference,
    })
}

struct InputDependentStage {
    revision: u32,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl acyclic_harness::context::ContextStage for InputDependentStage {
    fn name(&self) -> &str {
        "input-dependent"
    }
    fn contract(
        &self,
        _: Option<&acyclic_harness::conversation::ContentIo>,
    ) -> Result<serde_json::Value> {
        Ok(serde_json::json!({ "name": self.name(), "revision": self.revision }))
    }
    fn apply<'a>(
        &'a self,
        input: &'a acyclic_harness::context::ContextInput,
        mut context: Context,
        _: Limits,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, Context>> {
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
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

fn assert_fresh_stage_requests(provider: &ScriptedModelProvider, limits: Limits) -> usize {
    let requests = provider.requests();
    let responses = requests
        .iter()
        .filter(|request| {
            request
                .messages
                .iter()
                .any(|message| message.role == ModelRole::System)
        })
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), 10);
    for (turn, request) in responses.iter().enumerate() {
        let systems = request
            .messages
            .iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>();
        assert_eq!(systems.len(), 1);
        let expected = if turn < 5 { "stage:1:" } else { "stage:2:" };
        assert!(
            matches!(systems.first().map(|message| &message.content), Some(ModelContent::Text(text)) if text.starts_with(expected))
        );
        assert!(request.messages.len() <= limits.context_messages);
    }
    assert!(requests.len() > responses.len());
    requests.len()
}

#[path = "../context_continuation/projection.rs"]
mod projection;

#[path = "../context_continuation/canonical.rs"]
mod canonical;

#[path = "../context_continuation/snapshot.rs"]
mod snapshot;

#[path = "../context_continuation/recovery.rs"]
mod recovery;
use recovery::*;

#[path = "../context_continuation/publication.rs"]
mod publication;
use publication::*;
