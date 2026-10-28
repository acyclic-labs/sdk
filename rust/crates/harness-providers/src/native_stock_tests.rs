//! Stock turns on the native agent host: one A4 agent over a memory stream and
//! filesystem, prompted with a staged message and driven to quiescence.
use super::*;
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_harness_core::{
    Capabilities, OperationId,
    agent::{
        self, AgentHost, AgentOutcome, AgentPath, AgentRecord as _, AgentState, ExecutorDriver,
        ForkAdopted, InboxRecord, IntentId, Message, Phase, Sender, StatusRecord, VolumeBinding,
    },
    context::{CompactionPolicy, ContextPipeline},
    conversation::{
        ContentAccess, ContentPublisher as _, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::{Executor, StockExecutor},
    filesystem::{
        FilesystemContentPublisher, FilesystemHost, FilesystemTurnHost, PRIVATE_VOLUME,
        TurnAdmission, TurnAuthority, TurnExecutors, TurnLimits,
    },
    resources::ProviderRef,
    tool::ToolRegistry,
};
use acyclic_stream::{MemoryStream, ReadRequest, StreamPath, StreamProvider as _};

const LIMITS: TurnLimits = TurnLimits {
    maximum_bytes: 65_536,
    max_steps: 1,
    history_turns: 8,
};

#[tokio::test]
async fn every_adapter_runs_a_stock_turn_on_the_native_agent_host() -> Result<()> {
    for (adapter, response) in [
        (
            Adapter::Chat,
            "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n",
        ),
        (
            Adapter::Anthropic,
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":5}}}\n\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\ndata: {\"type\":\"message_stop\"}\n\n",
        ),
        (
            Adapter::Responses,
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
        ),
        (
            Adapter::Gemini,
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"done\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":1}}\n\n",
        ),
    ] {
        let transport = Arc::new(Fixture::new(response));
        let provider = if adapter == Adapter::Responses {
            let mut registry = Registry::with_transport(
                Credentials::explicit(BTreeMap::from([(
                    "openai".into(),
                    "fixture-api-key".into(),
                )])),
                transport.clone(),
            );
            let catalog = tempfile::NamedTempFile::new().map_err(|_| invalid("fixture catalog"))?;
            std::fs::write(catalog.path(), include_str!("fixtures/luna-catalog.json"))
                .map_err(|_| invalid("fixture catalog"))?;
            registry.load_catalog(catalog.path())?;
            let selected = registry.resolve("openai/gpt-6-luna")?;
            assert_eq!(
                selected.context_capacity(selected.model())?.context_tokens,
                8192
            );
            selected
        } else {
            let (mut provider, _) = provider(adapter, 200);
            provider.transport =
                crate::deadline::Bounded::new(transport.clone(), crate::TimeoutSpec::DEFAULT);
            provider
        };
        let turn = run_stock_turn(
            provider,
            CompactionPolicy::Disabled,
            acyclic_fs::Fs::memory(),
        )
        .await?;
        assert_eq!(turn, StockTurn::Settled, "{adapter:?}");
        assert_eq!(transport.sent.load(Ordering::SeqCst), 1, "{adapter:?}");
    }
    Ok(())
}

/// How the agent's one prompted turn ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StockTurn {
    /// The turn settled into the conversation and the agent waits for input.
    Settled,
    /// The turn's step failed; the agent keeps its input queued.
    Failed(String),
}

/// Builds each turn's stock executor over the provider under test.
struct Stock {
    provider: Arc<crate::ResolvedModel>,
    policy: CompactionPolicy,
}

impl TurnExecutors for Stock {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a acyclic_harness_core::model::ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            let provider = Arc::clone(&self.provider) as Arc<dyn ModelProvider>;
            let provider = provider
                .bind_content(authority.content.reader.clone(), LIMITS.maximum_bytes)
                .unwrap_or(provider);
            Ok(Arc::new(
                StockExecutor::new(
                    self.provider.model().clone(),
                    provider,
                    ContextPipeline::default(),
                    ToolRegistry::default(),
                )
                .with_compaction_policy(self.policy.clone()),
            ) as Arc<dyn Executor>)
        })
    }
}

fn agent_error(error: agent::Error) -> Error {
    invalid(error.to_string())
}

/// Prompts one root agent once and runs the host until it is quiescent.
pub(crate) async fn run_stock_turn<O: acyclic_fs::AsyncObjectStore + 'static>(
    provider: crate::ResolvedModel,
    policy: CompactionPolicy,
    filesystem: acyclic_fs::Fs<acyclic_fs::MemoryAuthorityBackend, O>,
) -> Result<StockTurn> {
    let stream = Arc::new(MemoryStream::default());
    let storage = ProviderRef::new("fixture".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(filesystem, &storage, None)?;
    let root = AgentPath::root(&agent::AgentId::from_bytes([2; 16]));
    let owner = acyclic_harness_core::AgentId::from_bytes(root.id().to_bytes());
    let volume = VolumeRef::new(
        storage,
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityRoot::new(
        "fixture",
        [47; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "agents".into(),
        },
    );
    // The prompt body is staged on the agent's private volume under the same
    // conversation authority its turns read with.
    let audience = issuer.with_audience(Authority {
        kind: AggregateKind::Conversation,
        id: root.id().as_str().to_owned(),
    });
    let signed = audience.bootstrap_scope(
        "agent",
        &Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(owner),
        &mut None,
    )?;
    let publisher = FilesystemContentPublisher::new(
        Arc::clone(&host),
        &volume,
        &audience.verifier(&mut None)?,
        &signed,
        LIMITS.maximum_bytes,
        None,
    )?;
    let body = publisher
        .stage(
            OperationId::from_bytes([1; 16]),
            "inputs/question.txt",
            b"question",
            "text/plain",
            "prompt",
            ContentAccess::default(),
        )?
        .await?
        .0;
    let turns = FilesystemTurnHost::new(
        stream.clone(),
        host,
        issuer,
        TurnAdmission::LocalRoot,
        Arc::new(Stock {
            provider: Arc::new(provider),
            policy,
        }),
        LIMITS,
        Default::default(),
    );
    let agents = AgentHost::new(
        stream.clone(),
        ExecutorDriver::new(turns),
        ActorReference::new(b"fixture", b"agents", b"providers")
            .map_err(|error| invalid(error.to_string()))?,
        ActorLimits::new(60_000, 1 << 20, 1 << 16).map_err(|error| invalid(error.to_string()))?,
        Arc::new(SystemClock),
    )
    .map_err(agent_error)?;
    let intent = |text: &str| IntentId::try_from(text).map_err(agent_error);
    agents
        .create(
            &intent("root")?,
            ForkAdopted {
                agent: root.id(),
                parent: None,
                forked_at: 0,
                conversation_tail: 0,
                volumes: vec![VolumeBinding {
                    name: PRIVATE_VOLUME.into(),
                    volume,
                }],
                brief: None,
                policy: None,
            },
        )
        .await
        .map_err(agent_error)?;
    let from = Sender::User {
        principal: "fixture".into(),
    };
    agents
        .send(
            &root,
            &from,
            &InboxRecord::Message(Message {
                from: from.clone(),
                id: intent("question")?,
                body: Box::new(body),
            }),
        )
        .await
        .map_err(agent_error)?;
    let failures = agents.runtime().drain().await;
    if let [(_, failure), ..] = failures.as_slice() {
        return Ok(StockTurn::Failed(failure.to_string()));
    }
    let status = records(&*stream, root.status().map_err(agent_error)?).await?;
    let last = status
        .last()
        .ok_or_else(|| invalid("the agent wrote no status"))?;
    let status = StatusRecord::decode(last).map_err(agent_error)?;
    let conversation = records(&*stream, root.conversation().map_err(agent_error)?).await?;
    Ok(match status.phase {
        Phase::Idle if conversation.len() == 1 => StockTurn::Settled,
        Phase::Done {
            outcome: AgentOutcome::Failed { message },
        } => StockTurn::Failed(message),
        phase => {
            return Err(invalid(format!(
                "the prompted turn did not settle: {phase:?}, {} conversation records",
                conversation.len()
            )));
        }
    })
}

/// The records of `path`; a stream no commit created yet has none.
async fn records(stream: &MemoryStream, path: StreamPath) -> Result<Vec<Vec<u8>>> {
    let records = match stream
        .read(ReadRequest {
            key: None,
            path,
            from: 0,
            limit: 64,
            at_least: Default::default(),
        })
        .await
    {
        Ok(records) => records,
        Err(acyclic_stream::StreamError::NotFound) => return Ok(Vec::new()),
        Err(error) => return Err(invalid(error.to_string())),
    };
    records
        .map_ok(|record| record.value.to_vec())
        .try_collect()
        .await
        .map_err(|error| invalid(error.to_string()))
}
