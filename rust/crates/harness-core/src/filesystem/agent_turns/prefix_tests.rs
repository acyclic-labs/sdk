use super::*;
use crate::{
    agent::{AgentRecord, ContextCut, ForkAdopted, HistoryCheckpoint, TurnOutcome},
    context::{CompactionPolicy, ContextPipeline, ModelContextCapacity, ModelTokenCount},
    conversation::Limits,
    executor::{ExecutionEvent, ExecutionJournal, ModelPurpose, StockExecutor},
    model::{Model, PreparedModelRequest},
    test_support::{ScriptedModelProvider, ScriptedTurn},
    tool::ToolRegistry,
};
use acyclic_stream::{AppendRequest, IdempotencyKey};
use std::sync::atomic::Ordering;

struct Executors(Arc<dyn Executor>);
impl TurnExecutors for Executors {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        _: &'a TurnAuthority,
    ) -> BoxFuture<'a, Result<Arc<dyn Executor>>> {
        Box::pin(async { Ok(Arc::clone(&self.0)) })
    }
}

type Host = FilesystemTurnHost<
    OpeningRead,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

async fn state(
    host: &Host,
    path: AgentPath,
    parent: Option<AgentPath>,
    tail: u64,
) -> Result<AgentState> {
    let volume = private_volume(host.host.provider.clone(), &path.id())?;
    host.host.create_volume(&volume).await?;
    let journal = crate::conversation::journal_volume(host.host.provider.clone(), &path.id())?;
    host.host.create_volume(&journal).await?;
    AgentState::adopted(
        path.clone(),
        &ForkAdopted {
            agent: path.id(),
            parent,
            profile: None,
            forked_at: 0,
            conversation_tail: tail,
            checkpoint: None,
            volumes: vec![WorkspaceEntry::initial_volume(
                crate::agent::WorkspaceRole::Private,
                PRIVATE_VOLUME.into(),
                volume,
            )],
            brief: None,
            policy: None,
        },
    )
    .map_err(Error::from)
}

fn executor() -> Result<StockExecutor> {
    let model = Model::new("fixture".into(), "prefix".into(), "1".into())?;
    let scripted = ScriptedModelProvider::new([ScriptedTurn::Text("answer".into())])
        .with_context_capacity(ModelContextCapacity {
            context_tokens: 65_536,
            output_tokens: Some(64),
        })
        .with_token_counter(|request| {
            Ok(ModelTokenCount::from_upper_bounds(
                request.request_digest(),
                1,
                vec![1; request.request().messages.len()],
            ))
        });
    Ok(StockExecutor::new(
        model,
        Arc::new(scripted),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_compaction_policy(CompactionPolicy::Disabled))
}

async fn fixture() -> Result<(Host, AgentState)> {
    let provider = ProviderRef::new("prefix".into(), "filesystem".into(), "2".into(), &mut None)?;
    let files = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let stream = Arc::new(OpeningRead {
        inner: Default::default(),
        pending: Default::default(),
        index_reads: Default::default(),
    });
    let host = FilesystemTurnHost::new(
        stream,
        acyclic_actors::addressing::ActorReference::new(b"prefix", b"agents", b"host")
            .map_err(|error| Error::Invalid(error.to_string()))?,
        files,
        AuthorityRoot::new(
            "prefix",
            [85; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "prefix".into(),
            },
        ),
        TurnAdmission::LocalRoot,
        Arc::new(Executors(Arc::new(executor()?))),
        TurnLimits {
            maximum_bytes: Some(1024 * 1024),
            history_turns: 8,
        },
        Default::default(),
    )
    .with_goals(None);
    let agent = state(
        &host,
        AgentPath::root(&crate::agent::AgentId::from_bytes([86; 16])),
        None,
        0,
    )
    .await?;
    Ok((host, agent))
}

async fn append(host: &Host, path: acyclic_stream::StreamPath, bytes: Vec<u8>) -> Result<()> {
    host.stream
        .append(AppendRequest {
            idempotency_key: IdempotencyKey::new(path.as_str())?,
            path,
            if_tail: Some(0),
            records: vec![bytes.into()],
        })
        .await?;
    Ok(())
}

async fn history(
    host: &Host,
    agent: &AgentState,
    context: acyclic_stream::StreamPath,
) -> Result<()> {
    let checkpoint = HistoryCheckpoint {
        live_children: false,
        version: 3,
        conversation: 1,
        checkpoint: None,
        context: ContextCut {
            path: context,
            tail: 1,
            checkpoint: None,
        },
        volumes: Vec::new(),
    };
    let mut baseline = checkpoint.clone();
    baseline.conversation = 0;
    baseline.context.tail = 0;
    append(
        host,
        agent.path.history().map_err(Error::from)?,
        crate::contract::canonical_json_bytes(&baseline)?,
    )
    .await?;
    append(
        host,
        agent.path.history().map_err(Error::from)?,
        crate::contract::canonical_json_bytes(&checkpoint)?,
    )
    .await
}

async fn seed(
    host: &Host,
    agent: &AgentState,
) -> Result<(PreparedModelRequest, serde_json::Value)> {
    let (journal, bindings, _files) = host
        .journal(host.admit(agent)?, &[], &[], None, None)
        .await?;
    let (body, _hold) = bindings
        .content
        .writer
        .as_ref()
        .unwrap()
        .stage_once(
            OperationId::new(),
            "brief.txt",
            b"earlier input",
            "text/plain",
            "brief.txt",
            Default::default(),
        )?
        .await?;
    let input = crate::agent::TurnInput::Brief {
        brief: Box::new(body),
    };
    let turn = crate::agent::turn_id(agent, &input).map_err(Error::from)?;
    let output = executor()?
        .execute(
            crate::executor::TurnInput {
                max_steps: None,
                operation_id: turn,
                input: ModelContent::Text("prefix".into()),
                selected_context: None,
            },
            &journal,
        )
        .await?;
    let output = crate::agent::TurnOutputs::stage(
        bindings.journal.files.as_ref(),
        turn,
        &crate::contract::canonical_json_bytes(&output)?,
    )
    .await?;
    let entry = ConversationEntry {
        observed_through: 0,
        steering: None,
        inbox: 0,
        input,
        output,
        outcome: TurnOutcome::Completed,
        goal: None,
        announcements: Vec::new(),
        tool_results: None,
        notices: None,
    };
    append(
        host,
        agent.path.conversation().map_err(Error::from)?,
        entry.encode().map_err(Error::from)?,
    )
    .await?;
    let records = journal.replay(turn, 0, 32, None)?.await?;
    let reference = records
        .into_iter()
        .find_map(|record| match record.event {
            ExecutionEvent::ModelStarted {
                purpose: ModelPurpose::Response,
                request,
                ..
            } => Some(request),
            _ => None,
        })
        .unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&journal.load(&reference, None)?.await?).unwrap();
    let request =
        crate::executor::load_model_request(&journal, &reference, Limits::default(), None).await?;
    Ok((request, manifest["messages"]["reference"].clone()))
}

async fn parent(
    journal: &dyn ExecutionJournal,
    request: &PreparedModelRequest,
) -> Result<serde_json::Value> {
    let reference = journal
        .stage_prepared_model_request(OperationId::new(), "request", request, None, None)?
        .unwrap()
        .await?;
    let manifest: serde_json::Value =
        serde_json::from_slice(&journal.load(&reference, None)?.await?).unwrap();
    let segment: FileRef =
        serde_json::from_value(manifest["messages"]["reference"].clone()).unwrap();
    let segment: serde_json::Value =
        serde_json::from_slice(&journal.load(&segment, None)?.await?).unwrap();
    Ok(segment["parent"]["reference"].clone())
}

#[tokio::test]
async fn opening_followup_reuses_own_request_and_inherited_child_reads_no_execution_index()
-> Result<()> {
    let (host, mut root) = fixture().await?;
    let (request, expected) = seed(&host, &root).await?;
    root.conversation_tail = 1;
    history(&host, &root, root.path.conversation().map_err(Error::from)?).await?;
    let mut continued = request.into_continuation();
    continued.messages().push(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text("next".into()),
    });
    let continued = continued.finish(Limits::default(), None)?;
    let open = host
        .open(
            &root,
            OperationId::new(),
            1,
            2,
            &ModelContent::Text("next".into()),
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?;
    assert_eq!(parent(&*open.journal, &continued).await?, expected);
    let child = state(
        &host,
        root.path
            .child(&crate::agent::AgentId::from_bytes([87; 16]))
            .map_err(Error::from)?,
        Some(root.path.clone()),
        1,
    )
    .await?;
    history(
        &host,
        &child,
        root.path.conversation().map_err(Error::from)?,
    )
    .await?;
    host.stream.index_reads.store(0, Ordering::Relaxed);
    let open = host
        .open(
            &child,
            OperationId::new(),
            1,
            2,
            &ModelContent::Text("child input".into()),
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        host.stream.index_reads.load(Ordering::Relaxed),
        0,
        "inherited history must not trigger a lookup in the child's execution index"
    );
    assert!(parent(&*open.journal, &continued).await?.is_null());
    Ok(())
}
