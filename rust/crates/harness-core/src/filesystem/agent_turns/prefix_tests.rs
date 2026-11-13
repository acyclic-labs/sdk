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
    executor_with_provider(ScriptedModelProvider::new([ScriptedTurn::Text(
        "answer".into(),
    )]))
}

fn executor_with_provider(scripted: ScriptedModelProvider) -> Result<StockExecutor> {
    let model = Model::new("fixture".into(), "prefix".into(), "1".into())?;
    let scripted = scripted
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
        version: 2,
        conversation: 1,
        checkpoint: None,
        context: ContextCut {
            path: context,
            tail: 1,
            checkpoint: None,
        },
        volumes: Vec::new(),
    };
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
    seed_with_executor(host, agent, executor()?).await
}

async fn seed_with_executor(
    host: &Host,
    agent: &AgentState,
    executor: StockExecutor,
) -> Result<(PreparedModelRequest, serde_json::Value)> {
    let (journal, bindings, _files) = host
        .journal((host.admit(agent)?, &agent.path), &[], &[], None, None)
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
    let output = executor
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
            crate::agent::TurnIdentity::new(OperationId::new(), 1, 2),
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
            crate::agent::TurnIdentity::new(OperationId::new(), 1, 2),
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

#[tokio::test]
async fn provider_attempt_uses_original_agent_trace_and_parented_journal() -> Result<()> {
    use acyclic_stream::trace::{Mechanism, Observation, Outcome, Registry, TraceQuery};
    let (host, agent) = fixture().await?;
    let registry = Registry::new(host.stream.clone());
    let source = crate::client_files::trace::source(&agent.path)?;
    let _interest = registry.subscribe(
        acyclic_stream::StreamPath::new(agent.path.as_str())?,
        source.clone(),
    )?;
    let host = host.with_trace_interest(registry);
    // Run the original stock executor, private filesystem publisher and durable
    // journal; only the external model responses are deterministic fixture data.
    let (release, resumed) = futures::channel::oneshot::channel::<()>();
    let (entered, observed) = futures::channel::oneshot::channel::<()>();
    let resumed = Arc::new(std::sync::Mutex::new(Some((resumed, entered))));
    let model = ScriptedModelProvider::new([ScriptedTurn::Text("answer".into())])
        .with_dispatch_observer(move |_| {
            let (resumed, entered) = resumed.lock().unwrap().take().unwrap();
            Box::pin(async move {
                assert!(entered.send(()).is_ok());
                resumed.await.map_err(|cause| {
                    Error::exchange(crate::ExchangeStep::Step("provider event gate"), cause)
                })
            })
        });
    let run = Box::pin(seed_with_executor(
        &host,
        &agent,
        executor_with_provider(model)?,
    ));
    match futures::future::select(observed, run).await {
        futures::future::Either::Left((entered, run)) => {
            entered.map_err(|cause| {
                Error::exchange(crate::ExchangeStep::Step("provider entry"), cause)
            })?;
            assert!(release.send(()).is_ok());
            run.await?;
        }
        futures::future::Either::Right((result, _)) => {
            result?;
            return Err(Error::Invalid("provider bypassed its event gate".into()));
        }
    }
    let traces = acyclic_stream::Stream::new(host.stream.clone(), source.as_str())?;
    let mut query = TraceQuery::new(
        traces.follow(0, Default::default()).await?,
        Default::default(),
    );
    let mut provider = None;
    let mut children = 0;
    let mut delivered_waits = 0;
    loop {
        match query.next().await? {
            Some(Observation::Started { id, .. }) => {
                let (_, start) = query
                    .active()
                    .find(|(candidate, _)| **candidate == id)
                    .ok_or_else(|| Error::Invalid("admitted trace start missing".into()))?;
                if start.name == "harness.provider_call" {
                    assert_eq!(start.mechanism, Mechanism::ProviderCall);
                    assert_eq!(
                        start.guarantee.as_deref(),
                        Some("journaled_model_attempt_settlement")
                    );
                    provider = Some(id);
                } else if provider.is_some() && start.parent == provider {
                    children += 1;
                }
            }
            Some(Observation::Completed { start, result, .. })
                if start.name == "harness.provider_event_wait" =>
            {
                assert_eq!(start.parent, provider);
                assert_eq!(start.mechanism, Mechanism::Wait);
                assert_eq!(start.guarantee.as_deref(), Some("provider_event_delivery"));
                assert_eq!(result, Outcome::Succeeded);
                delivered_waits += 1;
            }
            Some(Observation::Completed { id, result, .. }) if Some(id) == provider => {
                assert_eq!(result, Outcome::Succeeded);
                assert_eq!(delivered_waits, 1);
                assert!(
                    children > 0,
                    "provider work lost its original journal parent"
                );
                break;
            }
            None => {
                return Err(Error::Invalid(
                    "provider trace ended before settlement".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}
