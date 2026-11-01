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

    fn publication_details(&self) -> bool {
        true
    }

    fn record_owned(&self, sample: OwnedSample) {
        if sample.metadata.name() == OPERATION {
            self.0.record_owned(sample);
        }
    }
}

async fn journal() -> Result<(
    crate::filesystem::FilesystemExecutionJournal<
        acyclic_stream::MemoryStream,
        acyclic_fs::MemoryAuthorityBackend,
        acyclic_fs::MemoryObjectBackend,
    >,
    crate::core::AuthorityRoot,
)> {
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
    let journal = FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host,
        FilesystemJournalBinding {
            volume: &volume,
            verifier: &issuer.verifier(&mut None)?,
            scope: &scope,
            io: None,
        },
    )?;
    Ok((journal, issuer))
}

#[tokio::test]
async fn original_publications_match_receipts_and_preserve_observation_failures() -> Result<()> {
    use crate::executor::phase::Publication;
    use acyclic_observability::Outcome;

    complete_failures_obey_pinned_render_limits().await?;
    for rejected in [false, true] {
        for mode in 0..4 {
            let (journal, _issuer) = journal().await?;
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
                Model::new("fixture".into(), "publications".into(), "1".into())?,
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
            let (export, mut receiver) = Export::channel(credit.clone(), mode == 3);
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
                max_steps: Some(2),
            };
            let result = executor
                .execute(input.clone(), &journal)
                .await
                .unwrap_or_else(|error| {
                    panic!("fresh execution (rejected={rejected}, mode={mode}): {error:?}")
                });
            assert_eq!(result.text, "answer");
            let replayed = executor
                .execute(input, &journal)
                .await
                .unwrap_or_else(|error| {
                    panic!("completed replay (rejected={rejected}, mode={mode}): {error:?}")
                });
            assert_eq!(replayed, result);
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
                let sample = record.sample();
                if mode == 3 {
                    use acyclic_observability::{
                        PublicationControl, PublicationData, PublicationPurpose,
                    };
                    let (purpose, data) = match sample.rpc {
                        Some("model-dispatch") => (
                            PublicationPurpose::ModelDispatch,
                            PublicationData::PreparedRequest,
                        ),
                        Some("tool-intent") => (
                            PublicationPurpose::ToolIntent,
                            PublicationData::RecordedInvocation,
                        ),
                        Some("tool-settlement") => (
                            PublicationPurpose::ToolSettlement,
                            PublicationData::ResultAndProjection,
                        ),
                        Some("tool-failure-record") => (
                            PublicationPurpose::ToolFailureRecord,
                            PublicationData::InlineFailure,
                        ),
                        other => panic!("unexpected original publication: {other:?}"),
                    };
                    assert_eq!(
                        sample.publication,
                        Some(acyclic_observability::Publication {
                            purpose: Some(purpose),
                            data: Some(data),
                            control: Some(PublicationControl::ConditionalJournalAppend),
                        })
                    );
                } else {
                    assert!(
                        sample.publication.is_none(),
                        "detail opt-out retained publication"
                    );
                }
                exported.push(record);
            }
            assert_eq!(exported.len(), if mode >= 2 { 4 } else { 0 });
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
                let ExecutionEvent::ToolCompleted { step, call_id, .. } = &terminal.event else {
                    unreachable!();
                };
                let key =
                    tool_settlement_key(*step, call_id, ToolTerminalOutcome::Completed, None)?;
                // A replay record carries the authenticated retry digest,
                // rather than the original caller's settlement key.
                assert!(matches!(
                    journal
                        .append(operation, &terminal.idempotency_key, &terminal.event, None)?
                        .await,
                    Err(Error::Conflict(_))
                ));
                // Restore a duplicate logical observation around a real exact-key
                // replay. The journal remains correct; the cost oracle rejects it.
                Publication::ToolSettlement
                    .observe(
                        operation,
                        0,
                        journal.append(operation, &key, &terminal.event, None)?,
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
    publication_field_controls().await;
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

async fn complete_failures_obey_pinned_render_limits() -> Result<()> {
    struct DiagnosticTool(&'static str, usize, AtomicUsize);
    impl crate::tool::ToolExecutor for DiagnosticTool {
        fn execute<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<crate::runtime::ToolContext>,
        ) -> BoxFuture<'a, Result<ToolResult>> {
            self.2.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Err(Error::Invalid(self.0.repeat(self.1))) })
        }

        fn reconcile<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<crate::runtime::ToolContext>,
        ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            Box::pin(async { Ok(None) })
        }
    }

    // Both the complete text and its serialized result envelope must fit.
    for (text, count, render_bytes, admitted) in [
        ("x", 8192, 4096, false),
        ("\"", 2048, 4096, false),
        ("x", 8192, 16384, true),
    ] {
        let (journal, _issuer) = journal().await?;
        let model = Arc::new(ScriptedModelProvider::new([
            tool_turn([ModelEvent::ToolCall(crate::model::ToolCall::new(
                "call".into(),
                "example.echo".into(),
                json!({}),
            ))]),
            ScriptedTurn::Text("answer".into()),
        ]));
        let tool = Arc::new(DiagnosticTool(text, count, AtomicUsize::new(0)));
        let limits = Limits {
            render_bytes: Some(render_bytes),
            ..Limits::default()
        };
        let executor = uncompacted_executor(
            Model::new("fixture".into(), "failure-limits".into(), "1".into())?,
            model.clone(),
            ContextPipeline::default(),
            echo_tools(tool.clone(), json!(true), json!(true), json!(true))?,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["model:generate", "tool:call:example.echo"]),
                limits,
            )?,
            allowed()?,
        )?
        .with_limits(limits);
        let input = TurnInput {
            operation_id: OperationId::from_bytes([65; 16]),
            input: ModelContent::Text("input".into()),
            selected_context: None,
            max_steps: Some(2),
        };
        let result = executor.execute(input.clone(), &journal).await;
        let records = journal.replay(input.operation_id, 0, 100, None)?.await?;
        let failures: Vec<_> = records
            .iter()
            .filter_map(|record| match &record.event {
                ExecutionEvent::ToolFailed { cause, .. } => Some(cause),
                _ => None,
            })
            .collect();
        assert_eq!(tool.2.load(Ordering::SeqCst), 1);
        assert_eq!(failures.len(), usize::from(admitted));
        if admitted {
            let result = result?;
            assert_eq!(result.text, "answer");
            assert!(failures[0].message.ends_with(&text.repeat(count)));
            let expected =
                ToolFailureText(ToolFailureKind::ExecutorRejected, failures[0]).to_string();
            assert_eq!(
                super::tools_1::tool_error_text(&model.requests()[1].messages, "call"),
                Some(expected)
            );
            assert_eq!(executor.execute(input, &journal).await?, result);
            assert_eq!(model.requests().len(), 2);
            assert_eq!(tool.2.load(Ordering::SeqCst), 1);
        } else {
            assert!(matches!(result, Err(Error::Invalid(_))), "{result:?}");
            assert_eq!(model.requests().len(), 1);
            // Refusal has not committed an unreplayable failure or a partial cause.
            assert!(
                records
                    .iter()
                    .any(|record| matches!(record.event, ExecutionEvent::ToolStarted { .. }))
            );
        }
    }
    Ok(())
}

// Controls exercise the same collector/credited output as the real journal
// cohort above. No synthetic value is accepted as a physical durability count.
async fn publication_field_controls() {
    use acyclic_observability::{
        Publication as Evidence, PublicationControl, PublicationData, PublicationPurpose,
    };
    let credit = Arc::new(tokio::sync::Semaphore::new(7));
    let (export, mut receiver) = Export::channel(credit.clone(), true);
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(Operations(export)));
    let guard = tracing::dispatcher::set_default(&dispatch);
    let expected = Evidence {
        purpose: Some(PublicationPurpose::ModelDispatch),
        data: Some(PublicationData::PreparedRequest),
        control: Some(PublicationControl::ConditionalJournalAppend),
    };
    let deferred = tracing::info_span!(
        "acyclic.harness.executor.publication",
        rpc = tracing::field::Empty,
        data = tracing::field::Empty,
        control = tracing::field::Empty,
        outcome = tracing::field::Empty,
    );
    deferred.record("rpc", "model-dispatch");
    deferred.record("data", "prepared-request");
    deferred.record("control", "conditional-journal-append");
    deferred.record("outcome", "ok");
    deferred.record("rpc", "unknown-purpose");
    deferred.record("data", "inline-failure");
    deferred.record("control", "unknown-control");
    deferred.record("outcome", "err");
    drop(deferred);
    let unknown = tracing::info_span!(
        "acyclic.harness.executor.publication",
        rpc = "model-dispatch",
        data = "unknown-data",
        control = tracing::field::Empty,
        outcome = "ok",
    );
    drop(unknown);
    let abandoned = tracing::info_span!(
        "acyclic.harness.executor.publication",
        rpc = "model-dispatch",
        data = "prepared-request",
        control = "conditional-journal-append",
        outcome = tracing::field::Empty,
    );
    drop(acyclic_observability::Completion::new(&abandoned));
    drop(abandoned);
    let foreign = tracing::info_span!(
        "acyclic.harness.executor.other",
        rpc = "model-dispatch",
        data = "prepared-request",
        control = "conditional-journal-append",
        outcome = "ok",
    );
    drop(foreign);
    let immediate = tracing::info_span!(
        "acyclic.harness.executor.publication",
        rpc = "model-dispatch",
        data = "prepared-request",
        control = "conditional-journal-append",
        outcome = "ok",
    );
    drop(immediate);
    for unknown in [true, false] {
        let changed = tracing::info_span!(
            "acyclic.harness.executor.publication",
            rpc = "model-dispatch",
            data = "prepared-request",
            control = "conditional-journal-append",
            outcome = tracing::field::Empty,
        );
        if unknown {
            changed.record("rpc", "unknown-purpose");
            changed.record("data", "unknown-data");
            changed.record("control", "unknown-control");
        } else {
            changed.record("rpc", "tool-settlement");
            changed.record("data", "result-and-projection");
        }
        changed.record("outcome", "ok");
        drop(changed);
    }
    drop(guard);
    drop(dispatch);
    let mut records = Vec::new();
    while let Some(record) = receiver.recv().await {
        records.push(record);
    }
    assert_eq!(records.len(), 7);
    assert_eq!(records[0].sample().publication, Some(expected));
    assert_eq!(
        records[0].sample().outcome,
        acyclic_observability::Outcome::Ok
    );
    assert_eq!(
        records[1].sample().publication,
        Some(Evidence {
            purpose: expected.purpose,
            data: None,
            control: None,
        })
    );
    assert_eq!(records[2].sample().publication, Some(expected));
    assert_eq!(
        records[2].sample().outcome,
        acyclic_observability::Outcome::Incomplete
    );
    assert!(records[3].sample().publication.is_none());
    assert_eq!(records[4].sample().publication, Some(expected));
    assert_eq!(records[5].sample().publication, Some(Evidence::default()));
    assert_eq!(
        records[6].sample().publication,
        Some(Evidence {
            purpose: Some(PublicationPurpose::ToolSettlement),
            data: Some(PublicationData::ResultAndProjection),
            control: expected.control,
        })
    );
    assert_eq!(credit.available_permits(), 0);
    drop(records);
    assert_eq!(credit.available_permits(), 7);
}
