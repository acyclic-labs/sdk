use super::*;
use crate::config::{AdmissionStatus, utc};
use acyclic_stream::{
    MemoryStream, Stream, StreamProvider, StreamProviderExt, WriteOutcome, WriteRequest,
};
use futures::TryStreamExt as _;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

fn work() -> Result<acyclic_fs::NativeMaterializationWork> {
    acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs_work::performance::BOUNDARY_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

/// Commit time, set by each test before the write it dates.
#[derive(Default)]
struct Clock(AtomicU64);
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn memory() -> (Arc<MemoryStream>, Arc<Clock>) {
    let clock = Arc::new(Clock::default());
    let stream = MemoryStream::new_with_clock(Default::default(), clock.clone());
    (Arc::new(stream), clock)
}

fn admission(status: AdmissionStatus, error: Option<&str>, at: u64) -> Admission {
    Admission {
        path: "agent.ts".into(),
        version: "original".into(),
        status,
        error: error.map(Into::into),
        at: utc(at),
    }
}

async fn decide<P: crate::store::JournalProvider + ?Sized>(
    journal: SettingsAdmissions<'_, P>,
    agent: AgentId,
    cut: &str,
    decision: &Admission,
    maximum_bytes: Option<u64>,
    io: &crate::executor::PreparationIo,
) -> Result<()> {
    journal
        .record(
            agent,
            (&decision.path, cut, &decision.version),
            (
                decision.status,
                decision
                    .error
                    .as_ref()
                    .map(|error| error as &(dyn std::fmt::Display + Sync)),
            ),
            maximum_bytes,
            io,
        )
        .await
}

fn stored(record: &acyclic_stream::Record) -> Result<Outcome<String, String>> {
    let decision: Decision =
        serde_json::from_slice(&record.value).map_err(|error| Error::Storage(error.to_string()))?;
    let Decision::Inline { admission, .. } = decision else {
        return Err(Error::Storage("small admission was not inline".into()));
    };
    Ok(admission)
}

#[tokio::test]
async fn decisions_are_indexed_live_and_retained_without_an_agent_root() -> Result<()> {
    let (provider, _) = memory();
    let journal = SettingsAdmissions::new(&provider);
    let agent = AgentId::new();
    let original = admission(AdmissionStatus::Applied, None, 0);
    let work = work()?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    decide(journal, agent, "cut-a", &original, None, &io).await?;
    let path = SettingsAdmissions::path(agent)?;
    assert_eq!(provider.tail(path.clone()).await.map_err(invalid)?, 1);
    let mut other = original.clone();
    other.path = "plugins.ts".into();
    decide(journal, agent, "cut-a", &other, None, &io).await?;
    assert_eq!(
        journal
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(original.clone())
    );
    assert!(
        journal
            .latest(AgentId::new(), "agent.ts", "cut-a", "original", &work)
            .await?
            .is_none()
    );
    assert!(
        journal
            .latest(agent, "agent.ts", "cut-a", "different", &work)
            .await?
            .is_none()
    );
    let stream = Stream::new(provider.clone(), path.as_str()).map_err(invalid)?;
    let mut follow = stream
        .follow(2, Default::default())
        .await
        .map_err(invalid)?;
    let refused = admission(AdmissionStatus::Rejected, Some("model refused"), 0);
    decide(journal, agent, "cut-a", &refused, None, &io).await?;
    let event = follow.try_next().await.map_err(invalid)?.unwrap();
    assert_eq!(stored(&event)?.status, AdmissionStatus::Rejected);
    assert_eq!(event.sequence, 2);
    let reopened = SettingsAdmissions::new(&provider);
    assert_eq!(
        reopened
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(refused.clone())
    );
    // All prior decisions remain in the same ordinary, bounded history query.
    assert_eq!(provider.tail(path.clone()).await.map_err(invalid)?, 3);
    let mut history = stream.read(0, 3).await.map_err(invalid)?;
    let first = history.try_next().await.map_err(invalid)?.unwrap();
    assert_eq!(stored(&first)?.status, AdmissionStatus::Applied);
    assert_eq!(
        decide(journal, agent, "cut-a", &original, Some(1), &io)
            .await
            .err()
            .expect("oversized admission must refuse")
            .code(),
        crate::wire::ErrorCode::Invalid
    );
    assert_eq!(
        provider.tail(path.clone()).await.map_err(invalid)?,
        3,
        "bound refusal must not publish"
    );
    // The same file version under another complete source cut has its own
    // outcome. A path/version-only index would redirect both reads here.
    decide(journal, agent, "cut-b", &original, None, &io).await?;
    assert_eq!(
        reopened
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(refused)
    );
    assert_eq!(
        reopened
            .latest(agent, "agent.ts", "cut-b", "original", &work)
            .await?,
        Some(original)
    );
    assert!(
        reopened
            .latest(agent, "agent.ts", "missing-cut", "original", &work)
            .await?
            .is_none()
    );
    assert_eq!(provider.tail(path).await.map_err(invalid)?, 4);
    Ok(())
}

/// The replay contract: an unchanged decision appends nothing, so its time
/// stays the time it was first decided; a changed one is ordered after the
/// decision it replaces, including a return to an earlier outcome.
#[tokio::test]
async fn an_unchanged_decision_appends_nothing_and_a_changed_one_follows_the_one_it_replaces()
-> Result<()> {
    let (provider, clock) = memory();
    let journal = SettingsAdmissions::new(&provider);
    let agent = AgentId::new();
    let work = work()?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let path = SettingsAdmissions::path(agent)?;
    let applied = admission(AdmissionStatus::Applied, None, 100);
    for now in [100, 200, 300] {
        clock.0.store(now, Ordering::SeqCst);
        decide(journal, agent, "cut-a", &applied, None, &io).await?;
    }
    assert_eq!(
        provider.tail(path.clone()).await.map_err(invalid)?,
        1,
        "an unchanged decision appended again"
    );
    assert_eq!(
        journal
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(applied),
        "an unchanged decision lost its original time"
    );
    // A, B, A: each is a change from the decision before it.
    clock.0.store(400, Ordering::SeqCst);
    let rejected = admission(AdmissionStatus::Rejected, Some("refused"), 400);
    decide(journal, agent, "cut-a", &rejected, None, &io).await?;
    clock.0.store(500, Ordering::SeqCst);
    let returned = admission(AdmissionStatus::Applied, None, 500);
    decide(journal, agent, "cut-a", &returned, None, &io).await?;
    assert_eq!(provider.tail(path.clone()).await.map_err(invalid)?, 3);
    assert_eq!(
        journal
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(returned)
    );
    let stream = Stream::new(provider.clone(), path.as_str()).map_err(invalid)?;
    let statuses = stream
        .read(0, 3)
        .await
        .map_err(invalid)?
        .map_err(invalid)
        .and_then(|record| async move { Ok(stored(&record)?.status) })
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        statuses,
        [
            AdmissionStatus::Applied,
            AdmissionStatus::Rejected,
            AdmissionStatus::Applied
        ]
    );
    Ok(())
}

/// Real Memory storage that publishes one competing record between the tail
/// observation and the keyed read, and counts the tail conflicts it returns.
#[derive(Default)]
struct Interleaved {
    inner: Arc<MemoryStream>,
    competing: Mutex<Option<AppendRequest>>,
    conflicts: AtomicU64,
}

#[async_trait::async_trait]
impl StreamProvider for Interleaved {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        self.inner.context()
    }
    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
        let heads = self.inner.heads(request).await?;
        let competing = self.competing.lock().unwrap().take();
        if let Some(competing) = competing {
            assert!(matches!(
                self.inner.append(competing).await?,
                AppendOutcome::Committed(_)
            ));
        }
        Ok(heads)
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> std::result::Result<
        Option<acyclic_stream::IdempotencyObservation>,
        acyclic_stream::StreamError,
    > {
        self.inner.inspect_idempotency(key).await
    }
    fn write(
        &self,
        request: WriteRequest,
    ) -> futures::future::BoxFuture<
        '_,
        std::result::Result<WriteOutcome, acyclic_stream::StreamError>,
    > {
        Box::pin(async move {
            let outcome = self.inner.write(request).await?;
            if matches!(
                outcome,
                WriteOutcome::Append(AppendOutcome::TailConflict { .. })
            ) {
                self.conflicts.fetch_add(1, Ordering::SeqCst);
            }
            Ok(outcome)
        })
    }
    async fn read(
        &self,
        request: ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> std::result::Result<acyclic_stream::FollowStream, acyclic_stream::StreamError> {
        self.inner.follow(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.inner.read_commit(id).await
    }
}
impl acyclic_stream::NativeStreamProvider for Interleaved {}

/// The decision `record` itself would publish, appended by another writer.
fn competing(
    agent: AgentId,
    decision: &Admission,
    name: &str,
    io: &crate::executor::PreparationIo,
) -> Result<AppendRequest> {
    let value = io.encode(
        &Decision::Inline {
            version: FORMAT,
            agent,
            cut: "cut-a",
            admission: &Outcome {
                path: decision.path.as_str(),
                version: decision.version.as_str(),
                status: decision.status,
                error: decision.error.as_ref(),
            },
        },
        None,
        true,
    )?;
    let keys = [
        key(
            SELECTION,
            &decision.path,
            Some(("cut-a", &decision.version)),
            None,
        )?,
        key(LATEST, &decision.path, None, None)?,
    ];
    Ok(AppendRequest {
        path: SettingsAdmissions::path(agent)?,
        records: vec![AppendRecord {
            value,
            headers: headers(keys)?,
        }],
        if_tail: None,
        idempotency_key: IdempotencyKey::new(name.to_owned()).map_err(invalid)?,
    })
}

#[tokio::test]
async fn a_decision_published_between_the_tail_and_the_keyed_read_conflicts_and_is_compared_again()
-> Result<()> {
    let (inner, _) = memory();
    let provider = Arc::new(Interleaved {
        inner: inner.clone(),
        ..Default::default()
    });
    let journal = SettingsAdmissions::new(&provider);
    let reader = SettingsAdmissions::new(&inner);
    let agent = AgentId::new();
    let work = work()?;
    let io = crate::executor::PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
    let path = SettingsAdmissions::path(agent)?;
    let applied = admission(AdmissionStatus::Applied, None, 0);
    decide(journal, agent, "cut-a", &applied, None, &io).await?;

    // Another writer publishes a different decision after the tail is read.
    // The keyed read cannot see it, so the conditional append must conflict
    // and the retry must order this decision after the competing one.
    let invalid_decision = admission(AdmissionStatus::Invalid, Some("other writer"), 0);
    *provider.competing.lock().unwrap() = Some(competing(
        agent,
        &invalid_decision,
        "competing-different",
        &io,
    )?);
    let rejected = admission(AdmissionStatus::Rejected, Some("this writer"), 0);
    decide(journal, agent, "cut-a", &rejected, None, &io).await?;
    assert_eq!(provider.conflicts.load(Ordering::SeqCst), 1);
    assert_eq!(inner.tail(path.clone()).await.map_err(invalid)?, 3);
    assert_eq!(
        reader
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(rejected)
    );

    // The competing writer publishes this same decision. After the conflict
    // the comparison sees it, so nothing further is appended.
    let same = admission(AdmissionStatus::Applied, None, 0);
    *provider.competing.lock().unwrap() = Some(competing(agent, &same, "competing-same", &io)?);
    decide(journal, agent, "cut-a", &same, None, &io).await?;
    assert_eq!(provider.conflicts.load(Ordering::SeqCst), 2);
    assert_eq!(inner.tail(path).await.map_err(invalid)?, 4);
    assert_eq!(
        reader
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(same)
    );
    Ok(())
}

#[tokio::test]
async fn large_decisions_replay_from_bounded_chunks_and_refuse_incomplete_payloads() -> Result<()> {
    let (provider, _) = memory();
    let journal = SettingsAdmissions::new(&provider);
    let agent = AgentId::new();
    // Raw chunks may split UTF-8 and escaped JSON tokens. Only the complete
    // retained value is decoded, so neither can shorten the original cause.
    let cause = "設定\"\n\\".repeat(20_000);
    let original = admission(AdmissionStatus::Invalid, Some(&cause), 0);
    let work = work()?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let mut retained = None;
    for _ in 0..2 {
        decide(journal, agent, "cut-a", &original, None, &io).await?;
        let live = work.live_allocation_bytes();
        if let Some(original) = retained {
            assert_eq!(live, original, "chunk replay must retire temporary storage");
        }
        retained = Some(live);
    }
    let root = SettingsAdmissions::path(agent)?;
    assert_eq!(provider.tail(root.clone()).await.map_err(invalid)?, 1);
    let stream = Stream::new(provider.clone(), root.as_str()).map_err(invalid)?;
    let mut records = stream.read(0, 1).await.map_err(invalid)?;
    let record = records.try_next().await.map_err(invalid)?.unwrap();
    assert!(
        record.value.len() + record.headers.payload_bytes() <= acyclic_stream::MAX_RECORD_BYTES
    );
    let Decision::Chunks {
        version: FORMAT,
        digest,
        bytes,
    } = serde_json::from_slice::<Decision>(&record.value)
        .map_err(|error| Error::Storage(error.to_string()))?
    else {
        return Err(Error::Storage("large admission was not chunked".into()));
    };
    let path = SettingsAdmissions::payload_path(agent, &digest, None)?;
    let count = bytes.div_ceil(acyclic_stream::MAX_RECORD_BYTES as u64);
    assert!(count > 1);
    assert_eq!(provider.tail(path.clone()).await.map_err(invalid)?, count);
    let reopened = SettingsAdmissions::new(&provider);
    assert_eq!(
        reopened
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(original.clone())
    );
    assert!(
        reopened
            .latest(agent, "agent.ts", "cut-b", "original", &work)
            .await?
            .is_none()
    );

    // A complete indexed reference cannot make a truncated or substituted
    // prefix valid. Its length and digest both bind the complete decision.
    for (name, digest, bytes) in [("short", digest, bytes - 1), ("missing", [0; 32], bytes)] {
        let query = SettingsAdmissions::query(agent, "agent.ts", "cut-a", "original", None)?;
        provider
            .append(AppendRequest {
                path: query.path,
                records: vec![AppendRecord {
                    value: serde_json::to_vec(&Decision::<Outcome<String, String>>::Chunks {
                        version: FORMAT,
                        digest,
                        bytes,
                    })
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into(),
                    headers: RecordHeaders::new(vec![query.key.unwrap().key], vec![])
                        .map_err(invalid)?,
                }],
                if_tail: None,
                idempotency_key: IdempotencyKey::new(name).map_err(invalid)?,
            })
            .await
            .map_err(invalid)?;
        let error = reopened
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await
            .expect_err("an incomplete payload must refuse");
        assert_eq!(error.code(), crate::wire::ErrorCode::Storage);
    }

    // Stage failure must precede the indexed publication. A conflicting raw
    // prefix is a real provider failure, rather than an injected append mock.
    let other = AgentId::new();
    let body = io.encode(
        &Decision::Inline {
            version: FORMAT,
            agent: other,
            cut: "cut-a",
            admission: &Outcome {
                path: original.path.as_str(),
                version: original.version.as_str(),
                status: original.status,
                error: original.error.as_ref(),
            },
        },
        None,
        true,
    )?;
    let path = SettingsAdmissions::payload_path(other, blake3::hash(&body).as_bytes(), None)?;
    provider
        .append(AppendRequest {
            path,
            records: vec![b"different prefix".to_vec().into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new("conflicting-prefix").map_err(invalid)?,
        })
        .await
        .map_err(invalid)?;
    let error = decide(journal, other, "cut-a", &original, None, &io)
        .await
        .expect_err("a conflicting prefix must refuse before index publication");
    assert_eq!(error.code(), crate::wire::ErrorCode::Storage);
    assert!(
        journal
            .latest(other, "agent.ts", "cut-a", "original", &work)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn indexed_decisions_refuse_substituted_identity_and_unknown_formats() -> Result<()> {
    for (format, wrong_agent, wrong_path, wrong_version, wrong_cut) in [
        (FORMAT - 1, false, false, false, false),
        (FORMAT + 1, false, false, false, false),
        (FORMAT, true, false, false, false),
        (FORMAT, false, true, false, false),
        (FORMAT, false, false, true, false),
        (FORMAT, false, false, false, true),
    ] {
        let (provider, _) = memory();
        let agent = AgentId::new();
        let query = SettingsAdmissions::query(agent, "agent.ts", "cut-a", "original", None)?;
        let decision = Decision::Inline {
            version: format,
            cut: if wrong_cut { "cut-b" } else { "cut-a" }.to_owned(),
            agent: if wrong_agent { AgentId::new() } else { agent },
            admission: Outcome::<String, String> {
                path: if wrong_path { "plugins.ts" } else { "agent.ts" }.into(),
                version: if wrong_version {
                    "substitute"
                } else {
                    "original"
                }
                .into(),
                status: AdmissionStatus::Applied,
                error: None,
            },
        };
        provider
            .append(AppendRequest {
                path: query.path,
                records: vec![AppendRecord {
                    value: serde_json::to_vec(&decision)
                        .map_err(|error| Error::Invalid(error.to_string()))?
                        .into(),
                    headers: RecordHeaders::new(vec![query.key.unwrap().key], vec![])
                        .map_err(invalid)?,
                }],
                if_tail: None,
                idempotency_key: IdempotencyKey::new("substitution").map_err(invalid)?,
            })
            .await
            .map_err(invalid)?;
        let result = SettingsAdmissions::new(&provider)
            .latest(agent, "agent.ts", "cut-a", "original", &work()?)
            .await;
        if format != FORMAT {
            assert!(matches!(result, Err(Error::Unsupported(_))));
        } else {
            assert!(matches!(result, Err(Error::Storage(_))));
        }
    }
    Ok(())
}
