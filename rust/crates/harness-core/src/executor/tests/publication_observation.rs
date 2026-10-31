//! Original executor publications through the real Filesystem/Stream journal.
use super::*;
use acyclic_observability::{Export, Metrics, Operations, OwnedSample, Sample, Sink};
use tracing_subscriber::layer::SubscriberExt as _;

const OPERATION: &str = "acyclic.harness.executor.publication";
const SELECTIONS: [&str; 4] = [
    "model-dispatch",
    "tool-intent",
    "tool-settlement",
    "tool-failure-record",
];

// Keep output credit specific to the publications being checked, while the
// original Registry still owns all intermediate parent spans.
#[derive(Clone)]
struct Publications(Export);

impl Sink for Publications {
    fn record(&self, sample: &Sample<'_>) {
        if sample.metadata.name() == OPERATION {
            self.0.record(sample);
        }
    }

    fn record_owned(&self, sample: OwnedSample) {
        if sample.metadata.name() == OPERATION {
            self.0.record_owned(sample);
        }
    }
}

async fn journal() -> Result<
    crate::filesystem::FilesystemExecutionJournal<
        acyclic_stream::MemoryStream,
        acyclic_fs::MemoryAuthorityBackend,
        acyclic_fs::MemoryObjectBackend,
    >,
> {
    use crate::{
        conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
        filesystem::{FilesystemExecutionJournal, FilesystemHost, FilesystemJournalBinding},
        resources::ProviderRef,
    };
    let provider = ProviderRef::new(
        "publications".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = crate::AgentId::from_bytes([61; 16]);
    let volume = VolumeRef::new(
        provider,
        "publications".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityRoot::new(
        "publications",
        [62; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::from_bytes([63; 16]).to_string(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "owner",
        &Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        &mut None,
    )?;
    FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host,
        FilesystemJournalBinding {
            volume: &volume,
            verifier: &issuer.verifier(&mut None)?,
            scope: &scope,
            io: None,
        },
    )
}

#[tokio::test]
async fn original_publications_match_receipts_and_preserve_observation_failures() -> Result<()> {
    use crate::executor::phase::Publication;
    use acyclic_observability::Outcome;

    for rejected in [false, true] {
        for mode in 0..3 {
            let journal = journal().await?;
            let model = Arc::new(ScriptedModelProvider::new([
                tool_turn([ModelEvent::ToolCall(crate::model::ToolCall::new(
                    "call".into(),
                    "example.echo".into(),
                    json!({}),
                ))]),
                ScriptedTurn::Text("answer".into()),
            ]));
            let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
            let executor = uncompacted_executor(
                Model::new(
                    "fixture".into(),
                    "publications".into(),
                    "1".into(),
                    Value::Null,
                )?,
                model.clone(),
                ContextPipeline::default(),
                echo_tools(tool.clone(), json!(true), json!(!rejected), json!(true))?,
            )
            .with_tool_authority(
                RuntimeScope::new(
                    Capabilities::new(["model:generate", "tool:call:example.echo"]),
                    Limits::default(),
                )?,
                allowed()?,
            )?;
            let metrics = Metrics::default();
            let queries = SELECTIONS.map(|rpc| metrics.query_rpc(OPERATION, rpc));
            let credit = Arc::new(tokio::sync::Semaphore::new(8));
            let (export, mut receiver) = Export::channel(credit.clone(), false);
            let dispatch = match mode {
                0 => tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()),
                1 => tracing::Dispatch::new(
                    tracing_subscriber::registry().with(Operations(metrics.clone())),
                ),
                _ => tracing::Dispatch::new(
                    tracing_subscriber::registry()
                        .with(Operations((metrics.clone(), Publications(export.clone())))),
                ),
            };
            let guard = tracing::dispatcher::set_default(&dispatch);
            let operation = OperationId::from_bytes([64; 16]);
            let input = TurnInput {
                operation_id: operation,
                input: ModelContent::Text("input".into()),
                selected_context: None,
                max_steps: 2,
            };
            let result = executor.execute(input.clone(), &journal).await?;
            assert_eq!(result.text, "answer");
            assert_eq!(executor.execute(input, &journal).await?, result);
            assert_eq!(tool.0.load(Ordering::SeqCst), 1);
            assert_eq!(model.requests().len(), 2);

            let records = journal.replay(operation, 0, 100, None)?.await?;
            let expected = [2, 1, u64::from(!rejected), u64::from(rejected)];
            let actual = [
                records
                    .iter()
                    .filter(|r| matches!(r.event, ExecutionEvent::ModelStarted { .. }))
                    .count() as u64,
                records
                    .iter()
                    .filter(|r| matches!(r.event, ExecutionEvent::ToolStarted { .. }))
                    .count() as u64,
                records
                    .iter()
                    .filter(|r| matches!(r.event, ExecutionEvent::ToolCompleted { .. }))
                    .count() as u64,
                records
                    .iter()
                    .filter(|r| matches!(r.event, ExecutionEvent::ToolFailed { .. }))
                    .count() as u64,
            ];
            assert_eq!(actual, expected);
            let counts = || queries.each_ref().map(|query| query.snapshot().ok);
            assert_eq!(counts(), if mode == 0 { [0; 4] } else { actual });
            assert!(queries.iter().all(|query| {
                let s = query.snapshot();
                s.err == 0 && s.incomplete == 0 && !s.overflowed
            }));
            drop(guard);
            drop(dispatch);
            drop(export);
            let mut exported = Vec::new();
            while let Some(record) = receiver.recv().await {
                assert_eq!(record.sample().outcome, Outcome::Ok);
                assert!(record.sample().parent_id.is_some());
                assert!(record.sample().durability.is_none());
                exported.push(record);
            }
            assert_eq!(exported.len(), if mode == 2 { 4 } else { 0 });
            assert_eq!(credit.available_permits() + exported.len(), 8);
            drop(exported);
            assert_eq!(credit.available_permits(), 8);

            if mode == 1 && !rejected {
                let dispatch = tracing::Dispatch::new(
                    tracing_subscriber::registry().with(Operations(metrics.clone())),
                );
                let _guard = tracing::dispatcher::set_default(&dispatch);
                let terminal = records
                    .iter()
                    .find(|r| matches!(r.event, ExecutionEvent::ToolCompleted { .. }))
                    .unwrap();
                // Restore a duplicate logical observation around a real exact-key
                // replay. The journal remains correct; the cost oracle rejects it.
                Publication::ToolSettlement
                    .observe(
                        operation,
                        0,
                        journal.append(
                            operation,
                            &terminal.idempotency_key,
                            &terminal.event,
                            None,
                        )?,
                    )
                    .await?;
                assert_eq!(journal.replay(operation, 0, 100, None)?.await?, records);
                assert_ne!(
                    counts(),
                    actual,
                    "duplicate publication escaped its count oracle"
                );

                let (producer, completion) = tokio::sync::oneshot::channel::<Result<()>>();
                let lost = Publication::ModelDispatch.observe(operation, 1, async move {
                    completion
                        .await
                        .map_err(|_| Error::Storage("publication producer closed".into()))?
                });
                drop(producer);
                assert!(
                    matches!(lost.await, Err(Error::Storage(message)) if message == "publication producer closed")
                );
                assert_eq!(queries[0].snapshot().err, 1);
                // Dropping an observer before its first poll must never fabricate
                // a successful accepted publication.
                drop(Publication::ModelDispatch.observe(
                    operation,
                    2,
                    std::future::pending::<Result<()>>(),
                ));
                assert_eq!(queries[0].snapshot().incomplete, 1);
                assert_eq!(model.requests().len(), 2);
                assert_eq!(tool.0.load(Ordering::SeqCst), 1);
            }
        }
    }
    let _guard = tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    futures::executor::block_on(async {});
    let mut result = None;
    let observed = acyclic_test_support::measure(|| {
        result = Some(futures::executor::block_on(
            Publication::ModelDispatch.observe(
                OperationId::from_bytes([65; 16]),
                0,
                std::future::ready(Ok(())),
            ),
        ));
    });
    result.expect("measured publication ran")?;
    assert_eq!(observed.count_total, 0, "disabled observation allocated");
    Ok(())
}
