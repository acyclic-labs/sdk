//! Real local Stream/Filesystem persistence through the same canonical loop.
use super::*;
use crate::contract::capability;
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};
use crate::{
    AgentId, Capabilities,
    bundle::{HarnessBuilder, HarnessBundle},
    context::{Context, ContextPipeline, ContextPlacement, ModelTokenCount, SourceStage},
    conversation::{VolumeClass, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemContentPublisher, FilesystemContentVerifier},
    model::Model,
    resources::ProviderRef,
    runtime::ContentBindings,
};

type LocalJournal = FilesystemExecutionJournal<
    acyclic_stream::LocalStream,
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
>;

fn counted_model(turns: usize) -> ScriptedModelProvider {
    ScriptedModelProvider::new(std::iter::repeat_n(
        ScriptedTurn::Text("durable summary or answer".into()),
        turns,
    ))
    .with_token_counter(|request| {
        Ok(ModelTokenCount::from_upper_bounds(
            request.request_digest(),
            512,
            vec![512; request.request().messages.len()],
        ))
    })
}

struct Binding {
    journal: Arc<LocalJournal>,
    bundle: HarnessBundle,
}

async fn open(
    root: &std::path::Path,
    agent: AgentId,
    model: Arc<ScriptedModelProvider>,
    initialize: bool,
) -> Result<Binding> {
    let limits = Limits {
        context_messages: 8,
        ..Limits::default()
    };
    let provider = ProviderRef::new(
        "durable-continuation".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let volume = VolumeRef::new(
        provider.clone(),
        "conversation".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let host = FilesystemHost::new(
        acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(root.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        &(provider),
        None,
    )?;
    if initialize {
        host.create_volume(&volume).await?;
    }
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "durable-10k".into(),
    };
    let issuer = AuthorityIssuer::new("durable-continuation", [58; 32], authority.clone());
    let read = volume.capability(VolumeOperation::Read, &mut None)?;
    let write = volume.capability(VolumeOperation::Write, &mut None)?;
    let scope = issuer.issue(
        "owner",
        &Capabilities::new([
            capability::CONVERSATION_BIND.to_owned(),
            capability::CONVERSATION_APPEND.to_owned(),
            capability::CONVERSATION_SELECT_CONTEXT.to_owned(),
            "model:generate".to_owned(),
            read.clone(),
            write.clone(),
        ]),
        Some(agent),
        None,
        &mut None,
    )?;
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(root.join("stream"), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    if initialize {
        let mut aggregate = StreamAggregate::open(
            &stream,
            (
                authority,
                issuer.verifier(&mut None)?,
                SchemaRegistry::new(),
            ),
            None,
            crate::store::default_projection_read_limits(),
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None,
        )
        .await?;
        aggregate
            .execute(
                Command {
                    operation_id: OperationId::new(),
                    idempotency_key: IdempotencyKey::new("bind-durable-10k")?,
                    expected_revision: 0,
                    scope: scope.clone(),
                    causal_parent: None,
                    action: Action::BindConversation { agent },
                },
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let reader = FilesystemContentVerifier::new(
        host.clone(),
        &issuer.verifier(&mut None)?,
        &scope,
        262_144,
        None,
    )?;
    let writer = FilesystemContentPublisher::new(
        host.clone(),
        &volume,
        &issuer.verifier(&mut None)?,
        &scope,
        262_144,
        None,
    )?;
    let journal = Arc::new(
        FilesystemExecutionJournal::new(
            stream,
            host,
            &crate::core::SchemaRegistry::new(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &volume,
                verifier: &issuer.verifier(&mut None)?,
                scope: &scope,
                maximum_bytes: 262_144,
                io: None,
            },
        )?
        .with_input_verifier(reader.clone()),
    );
    let instructions = Context {
        messages: [
            "Keep the user's constraints.",
            "Preserve original references.",
        ]
        .into_iter()
        .map(|text| crate::model::ModelMessage {
            role: crate::model::ModelRole::System,
            content: ModelContent::Text(text.into()),
        })
        .collect(),
        ..Context::default()
    };
    let bundle = HarnessBuilder::new()
        .journal(journal.clone())
        .content(ContentBindings {
            reader,
            writer: Some(writer),
        })
        .model(
            Model::new(
                "synthetic".into(),
                "durable-continuation".into(),
                "1".into(),
                serde_json::Value::Null,
            )?,
            model,
        )
        .grant("model:generate")
        .grant(read)
        .grant(write)
        .limits(limits)
        .context(ContextPipeline::new([Arc::new(SourceStage::new(
            "instructions",
            "1",
            Arc::new(instructions),
            ContextPlacement::Prepend,
        ))
            as Arc<dyn crate::context::ContextStage>]))
        .build()?;
    Ok(Binding { journal, bundle })
}

#[tokio::test]
async fn durable_turn_growth_reopens_and_replays_original_identity() -> Result<()> {
    const N: u16 = 1;
    const WARMUP: u16 = 4;
    const TURNS: u16 = WARMUP + 10 * N;
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let agent = AgentId::new();
    let model = Arc::new(counted_model(usize::from(TURNS) * 2));
    let mut binding = open(directory.path(), agent, model.clone(), true).await?;
    let initial_operation = OperationId::new();
    let input = binding
        .journal
        .stage_conversation(
            initial_operation,
            "input.txt",
            b"small input",
            "text/plain",
            "input.txt",
        )
        .await?;
    let mut first = None;
    let mut observations = Vec::new();
    for index in 0..TURNS {
        let operation = if index == 0 {
            initial_operation
        } else {
            OperationId::new()
        };
        let output = binding
            .journal
            .run_conversation(&binding.bundle, operation, input.clone(), Vec::new(), 1)
            .await
            .map_err(|error| Error::Storage(format!("durable retained turn {index}: {error}")))?;
        if first.is_none() {
            first = Some((operation, input.clone(), output));
        }
        {
            let aggregate = binding
                .journal
                .conversation_projection(binding.bundle.limits())
                .await?;
            let state = aggregate
                .reducer()
                .conversation()
                .ok_or_else(|| Error::Storage("missing durable conversation".into()))?;
            assert_eq!(state.logical_revision(), u64::from(index + 1) * 2);
            assert!(state.messages().len() <= 8);
            if index >= 3 {
                assert!(state.resident_after_sequence() > 0);
            }
            if index + 1 == WARMUP + N || index + 1 == TURNS {
                let work = acyclic_fs::NativeMaterializationWork::new(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &acyclic_fs::CancellationToken::new(),
                )
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
                let (copy, _) = state.copy(&mut Some(&mut io.history_io()), None)?;
                assert_eq!(&copy, state);
                drop(copy);
                drop(io);
                observations.push(serde_json::to_value(work.receipt()).expect("work counters"));
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
    let [small, large] = observations.as_slice() else {
        return Err(Error::Invalid("missing growth observations".into()));
    };
    for (counter, value) in small.as_object().expect("work counters") {
        assert!(
            large
                .get(counter)
                .and_then(serde_json::Value::as_u64)
                .expect("counter")
                <= 10 * value.as_u64().expect("counter"),
            "{counter} grows faster than the workload: {small:?} -> {large:?}"
        );
    }
    let captured = model.requests();
    assert!(captured.iter().all(|request| request.messages.len() <= 8));
    assert!(model.reconciliations().is_empty());
    let requests = captured.len();
    assert!(
        requests > usize::from(TURNS),
        "count pressure must admit summaries"
    );
    let (operation, input, expected) =
        first.ok_or_else(|| Error::Invalid("no original durable turn".into()))?;
    let output = binding
        .journal
        .run_conversation(&binding.bundle, operation, input, Vec::new(), 1)
        .await?;
    assert_eq!(output, expected);
    assert_eq!(
        model.requests().len(),
        requests,
        "replay must not dispatch again"
    );
    Ok(())
}
