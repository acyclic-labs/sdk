//! Real local Stream/Filesystem persistence through the same canonical loop.
use super::*;
use crate::{
    AgentId, Capabilities,
    bundle::{HarnessBuilder, HarnessBundle},
    context::{Context, ContextPipeline, ContextPlacement, ModelContextCapacity, ModelTokenCount, SourceStage},
    conversation::{VolumeClass, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemContentPublisher, FilesystemContentVerifier},
    model::{Model, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
    resources::ProviderRef,
    runtime::ContentBindings,
};
use std::sync::atomic::{AtomicUsize, Ordering};

type LocalJournal = FilesystemExecutionJournal<acyclic_stream::LocalStream,
    acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;

#[derive(Default)]
struct CountedModel(AtomicUsize);
impl ModelProvider for CountedModel {
    fn context_capacity(&self, _: &Model) -> Result<ModelContextCapacity> {
        Ok(ModelContextCapacity { context_tokens: 131_072, output_tokens: 4_096 })
    }
    fn count_tokens(&self, request: &PreparedModelRequest) -> Result<ModelTokenCount> {
        Ok(ModelTokenCount {
            request_digest: request.request_digest(),
            fixed_tokens: 512,
            message_tokens: vec![512; request.request().messages.len()],
        })
    }
    fn generate<'a>(&'a self, request: PreparedModelRequest, dispatch: ModelDispatch)
        -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>>
    {
        assert_eq!(request.request_digest(), dispatch.request_digest);
        assert!(request.request().messages.len() <= 8);
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::iter([
            Ok(ModelEvent::Content { delta: "durable summary or answer".into() }),
            Ok(ModelEvent::Completed { metadata: serde_json::Value::Null }),
        ]))
    }
    fn reconcile<'a>(&'a self, _dispatch: crate::model::ModelDispatch, _observed: &'a [crate::model::ModelEvent])
        -> acyclic_stream::BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>>
    {
        Box::pin(async { Err(Error::Unsupported("all fixture attempts settle".into())) })
    }
}

struct Binding {
    journal: Arc<LocalJournal>,
    bundle: HarnessBundle,
}

async fn open(root: &std::path::Path, agent: AgentId, model: Arc<CountedModel>, initialize: bool)
    -> Result<Binding>
{
    let limits = Limits { context_messages: 8, ..Limits::default() };
    let provider = ProviderRef::new("durable-continuation", "filesystem", "2")?;
    let volume = VolumeRef::new(provider.clone(), "conversation", VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent))?;
    let host = Arc::new(FilesystemHost::new(
        acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(root.join("filesystem"))).await
            .map_err(|error| Error::Storage(error.to_string()))?, provider,
    )?);
    if initialize { host.create_volume(&volume).await?; }
    let authority = Authority { kind: AggregateKind::Conversation, id: "durable-10k".into() };
    let issuer = AuthorityIssuer::new("durable-continuation", [58; 32], authority.clone());
    let read = volume.capability(VolumeOperation::Read, &mut None)?;
    let write = volume.capability(VolumeOperation::Write, &mut None)?;
    let scope = issuer.issue("owner", &Capabilities::new([
        capability::CONVERSATION_BIND.to_owned(), capability::CONVERSATION_APPEND.to_owned(),
        capability::CONVERSATION_SELECT_CONTEXT.to_owned(), "model:generate".to_owned(),
        read.clone(), write.clone(),
    ]), Some(agent), None, &mut None).expect("scope proof");
    let stream = Arc::new(acyclic_stream::LocalStream::open(
        root.join("stream"), Default::default()).await
        .map_err(|error| Error::Storage(error.to_string()))?);
    if initialize {
        let mut aggregate = StreamAggregate::open(&stream, authority, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()).await?;
        aggregate.execute(Command {
            operation_id: OperationId::new(), idempotency_key: IdempotencyKey::new("bind-durable-10k")?,
            expected_revision: 0, scope: scope.clone(), causal_parent: None,
            action: Action::BindConversation { agent },
        }, #[cfg(feature = "filesystem")] None, &mut None).await?;
    }
    let reader = Arc::new(FilesystemContentVerifier::new(host.clone(), &issuer.verifier(&mut None).expect("verifier copy"),
        &scope, 262_144, None)?);
    let writer = Arc::new(FilesystemContentPublisher::new(host.clone(), &volume,
        &issuer.verifier(&mut None).expect("verifier copy"), &scope, 262_144, None)?);
    let journal = Arc::new(FilesystemExecutionJournal::new(stream, host, &volume,
        &issuer.verifier(&mut None).expect("verifier copy"), &crate::core::SchemaRegistry::new(), &scope, 262_144, None)?.with_input_verifier(reader.clone()));
    let instructions = Context {
        messages: ["Keep the user's constraints.", "Preserve original references."].into_iter()
            .map(|text| crate::model::ModelMessage { role: crate::model::ModelRole::System,
                content: ModelContent::Text(text.into()) }).collect(),
        ..Context::default()
    };
    let bundle = HarnessBuilder::new().journal(journal.clone())
        .content(ContentBindings { reader, writer: Some(writer) })
        .model(Model::new("synthetic", "durable-continuation", "1", serde_json::Value::Null)?, model)
        .grant("model:generate").grant(read).grant(write).limits(limits)
        .context(ContextPipeline::new([Arc::new(SourceStage::new("instructions", "1",
            Arc::new(instructions), ContextPlacement::Prepend)) as Arc<dyn crate::context::ContextStage>]))
        .build()?;
    Ok(Binding { journal, bundle })
}

#[tokio::test]
async fn durable_ten_thousand_turns_reopen_and_replay_original_identity() -> Result<()> {
    const TURNS: u16 = 10_000;
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let agent = AgentId::new();
    let model = Arc::new(CountedModel::default());
    let mut binding = open(directory.path(), agent, model.clone(), true).await?;
    let mut first = None;
    for index in 0..TURNS {
        let operation = OperationId::new();
        let input = binding.journal.stage_conversation(operation, &format!("turns/{index}/input.txt"),
            b"small input", "text/plain", "input.txt").await?;
        let output = binding.journal.run_conversation(&binding.bundle, operation, input.clone(), Vec::new(), 1)
            .await.map_err(|error| Error::Storage(format!("durable retained turn {index}: {error}")))?;
        if first.is_none() { first = Some((operation, input, output)); }
        {
            let aggregate = binding.journal.conversation_projection(binding.bundle.limits()).await?;
            let state = aggregate.reducer().conversation()
                .ok_or_else(|| Error::Storage("missing durable conversation".into()))?;
            assert_eq!(state.logical_revision(), u64::from(index + 1) * 2);
            assert!(state.messages().len() <= 8);
            if index >= 3 { assert!(state.resident_after_sequence() > 0); }
        }
        if index == 113 || index == TURNS - 1 {
            let workspace = crate::filesystem::workspace_ref(
                binding.journal.host.provider.clone(),
                &binding.journal.volume.storage_name()?,
            )?;
            for (path, expected_entries) in [
                ("/.system/execution", 1),
                ("/turns/.canonical", 1),
                ("/.system/harness-file-metadata", 2),
                ("/.system/harness-uploads", 2),
            ] {
                let page = binding.journal.host.list(&workspace, None, path, 3).await?;
                assert_eq!(page.entries.len(), expected_entries, "{path}");
                assert!(!page.has_more, "active namespace must not accumulate old turns");
            }
        }
        if index == TURNS - 3 {
            let weak_journal = Arc::downgrade(&binding.journal);
            let weak_files = Arc::downgrade(&binding.journal.host);
            drop(binding);
            assert!(weak_journal.upgrade().is_none());
            assert!(weak_files.upgrade().is_none());
            binding = open(directory.path(), agent, model.clone(), false).await?;
        }
    }
    let requests = model.0.load(Ordering::SeqCst);
    assert!(requests > usize::from(TURNS), "count pressure must admit summaries");
    let (operation, input, expected) = first.ok_or_else(|| Error::Invalid("no original durable turn".into()))?;
    let output = binding.journal.run_conversation(&binding.bundle, operation, input, Vec::new(), 1).await?;
    assert_eq!(output, expected);
    assert_eq!(model.0.load(Ordering::SeqCst), requests, "replay must not dispatch again");
    Ok(())
}
