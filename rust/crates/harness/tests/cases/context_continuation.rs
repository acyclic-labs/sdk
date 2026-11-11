//! Continuing projection publication through real Stream and private content.
#![cfg(feature = "filesystem-local")]

use acyclic_harness::context::{
    CompactionPolicy, CompactionReference, Context, ContextPipeline, ContextPlacement,
    ModelContextCapacity, ModelTokenCount,
};
use acyclic_harness::conversation::{ContentResidencyVerifier, FileRef, Limits};
use acyclic_harness::executor::{Executor, StockExecutor, TurnInput};
use acyclic_harness::filesystem::MemoryHarnessStorage;
use acyclic_harness::model::{
    FileProjectionPolicy, Model, ModelContent, ModelContentPart, ModelEvent, ModelMessage,
    ModelRequest, ModelRole, PreparedModelRequest,
};
use acyclic_harness::test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn};
use acyclic_harness::tool::{ToolDefinition, ToolRegistry};
use acyclic_harness::{AgentId, Error, OperationId, Result};
use acyclic_stream::MAX_RECORD_BYTES;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct StoredContent(Arc<MemoryHarnessStorage>);

fn summary_model(turns: usize) -> (Arc<ScriptedModelProvider>, Arc<AtomicUsize>) {
    summary_model_with_turns((0..turns).map(|_| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::Content {
                delta: "retained summary".into(),
            }),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ])
    }))
}

fn summary_model_with_turns(
    turns: impl IntoIterator<Item = ScriptedTurn>,
) -> (Arc<ScriptedModelProvider>, Arc<AtomicUsize>) {
    let counts = Arc::new(AtomicUsize::new(0));
    let counter = counts.clone();
    let provider = ScriptedModelProvider::new(turns)
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
                    let bytes = message.content.file_refs().try_fold(
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
        _ceiling: Option<&'a acyclic_harness::Capabilities>,
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
    storage: Arc<MemoryHarnessStorage>,
    source: Context,
    compacted: Context,
    reference: CompactionReference,
}

async fn fixture() -> Result<Fixture> {
    let storage = Arc::new(MemoryHarnessStorage::new(AgentId::new(), 65_536).await?);
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
        Model::new("synthetic".into(), "summary".into(), "1".into())?,
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
    let bytes = acyclic_harness::canonical_json_bytes_bounded(
        &source,
        acyclic_harness::JsonBounds {
            maximum_bytes: Some(65_536),
            maximum_items: Some(4_096),
            maximum_depth: 64,
        },
        None,
        &mut |_| Ok(()),
    )?;
    let compacted = Context {
        messages: vec![
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::File {
                    file: summary.output.clone(),
                    policy: FileProjectionPolicy::BoundedFull,
                }),
            },
            source.messages[2].clone(),
        ],
        current_input_index: Some(1),
        ..Context::default()
    };
    let reference = CompactionReference {
        source_digest: *blake3::hash(&bytes).as_bytes(),
        maximum_messages: 2,
        summary: Some(summary),
    };
    Ok(Fixture {
        storage,
        source,
        compacted,
        reference,
    })
}

#[path = "../context_continuation/projection.rs"]
mod projection;

#[path = "../context_continuation/snapshot.rs"]
mod snapshot;

#[path = "../context_continuation/recovery.rs"]
mod recovery;
