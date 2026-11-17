use super::*;
use acyclic_stream::{StreamProvider, WriteOutcome, WriteRequest};
use futures::{FutureExt as _, channel::oneshot};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        100
    }
}
impl acyclic_actors::local::Clock for Clock {
    fn sleep_until(
        &self,
        _: u64,
    ) -> acyclic_stream::BoxProviderFuture<
        'static,
        std::result::Result<(), acyclic_actors::local::ClockError>,
    > {
        Box::pin(async { Ok(()) })
    }
}

struct Storage {
    inner: acyclic_stream::MemoryStream,
    armed: AtomicBool,
    stalled: AtomicBool,
    observed: Mutex<Option<oneshot::Sender<()>>>,
}

impl Storage {
    async fn available(&self) {
        if self.stalled.load(Ordering::SeqCst) {
            if let Some(observed) = self.observed.lock().unwrap().take() {
                let _ = observed.send(());
            }
            std::future::pending::<()>().await;
        }
    }
}

#[async_trait::async_trait]
impl StreamProvider for Storage {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        self.inner.context()
    }

    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
        self.available().await;
        self.inner.heads(request).await
    }
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<
        Option<acyclic_stream::IdempotencyObservation>,
        acyclic_stream::StreamError,
    > {
        self.available().await;
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
            self.available().await;
            let outcome = self.inner.write(request).await?;
            if self.armed.load(Ordering::SeqCst)
                && matches!(
                    outcome,
                    WriteOutcome::Commit(acyclic_stream::CommitOutcome::Committed(_))
                        | WriteOutcome::CommitAccepted(Ok(..))
                )
            {
                self.stalled.store(true, Ordering::SeqCst);
            }
            Ok(outcome)
        })
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.available().await;
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> std::result::Result<acyclic_stream::FollowStream, acyclic_stream::StreamError> {
        self.available().await;
        self.inner.follow(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.available().await;
        self.inner.read_commit(id).await
    }
}
impl acyclic_stream::NativeStreamProvider for Storage {}

#[tokio::test]
async fn a_claim_returns_its_timestamp_when_storage_stalls_after_commit() -> Result<()> {
    let (observed, attempted) = oneshot::channel();
    let storage = Arc::new(Storage {
        inner: acyclic_stream::MemoryStream::new_with_clock(Default::default(), Arc::new(Clock)),
        armed: AtomicBool::new(false),
        stalled: AtomicBool::new(false),
        observed: Mutex::new(Some(observed)),
    });
    let (journal, _issuer) =
        test_support::journal_with_backends(storage.clone(), acyclic_fs::Fs::memory()).await?;
    let operation = OperationId::new();
    journal
        .append(
            operation,
            "start",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let invocation = journal
        .stage_with_io(operation, "invocation", b"{}", "application/json", None)
        .await?;
    let mut event = ExecutionEvent::ToolStarted {
        step: 0,
        call_id: "call".into(),
        invocation,
        publication: None,
        started_at_micros: None,
    };
    storage.armed.store(true, Ordering::SeqCst);
    let claim = journal.claim_if_tail(operation, 1, "claim", &mut event, None)?;
    match futures::future::select(claim, attempted).await {
        futures::future::Either::Left((timestamp, _)) => assert_eq!(timestamp?, Some(100_000)),
        futures::future::Either::Right(_) => {
            panic!("claim tried to discover its committed timestamp with storage unavailable")
        }
    }
    assert!(storage.stalled.load(Ordering::SeqCst));
    // Control: the storage really is held, rather than merely returning errors.
    assert!(
        journal
            .replay(operation, 0, 8, None)?
            .now_or_never()
            .is_none()
    );
    storage.stalled.store(false, Ordering::SeqCst);
    storage.armed.store(false, Ordering::SeqCst);
    let records = journal.replay(operation, 0, 8, None)?.await?;
    assert_eq!(
        records.last().and_then(|record| record.committed_at_micros),
        Some(100_000)
    );
    assert_eq!(
        journal
            .claim_if_tail(operation, 1, "claim", &mut event, None)?
            .await?,
        None,
        "an accepted claim cannot be acquired twice"
    );
    Ok(())
}

struct ProviderClock;
impl acyclic_stream::UnixMillisClock for ProviderClock {
    fn now_unix_millis(&self) -> u64 {
        200
    }
}

/// A claim stamps its start on the journal's clock and persists that same
/// value, so a resumed timed tool measures its deadline from the original
/// claim rather than the provider's frame time.
#[tokio::test]
async fn a_claim_persists_its_journal_clock_timestamp_for_resume() -> Result<()> {
    let storage = Arc::new(Storage {
        inner: acyclic_stream::MemoryStream::new_with_clock(
            Default::default(),
            Arc::new(ProviderClock),
        ),
        armed: AtomicBool::new(false),
        stalled: AtomicBool::new(false),
        observed: Mutex::new(None),
    });
    let (mut journal, _issuer) =
        test_support::journal_with_backends(storage, acyclic_fs::Fs::memory()).await?;
    journal.clock = Some(Arc::new(Clock));
    let operation = OperationId::new();
    journal
        .append(
            operation,
            "start",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let invocation = journal
        .stage_with_io(operation, "invocation", b"{}", "application/json", None)
        .await?;
    let mut event = ExecutionEvent::ToolStarted {
        step: 0,
        call_id: "call".into(),
        invocation,
        publication: None,
        started_at_micros: None,
    };
    assert_eq!(
        journal
            .claim_if_tail(operation, 1, "claim", &mut event, None)?
            .await?,
        Some(100_000)
    );
    let records = journal.replay(operation, 0, 8, None)?.await?;
    let started = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ToolStarted { .. } => {
                Some(serde_json::to_value(&record.event).expect("tool start JSON"))
            }
            _ => None,
        })
        .expect("the claim is replayed");
    assert_eq!(started["started_at_micros"], 100_000);
    assert_eq!(
        records.last().and_then(|record| record.committed_at_micros),
        Some(200_000),
        "the provider's frame time is not the claim's journal-clock time"
    );
    assert_eq!(
        journal
            .claim_if_tail(operation, 1, "claim", &mut event, None)?
            .await?,
        None,
        "a repeated claim returns no timestamp"
    );
    Ok(())
}

/// A claim that loses the expected tail is refused, and a refused claim
/// returns no timestamp: its caller must not run a tool it never claimed.
#[tokio::test]
async fn a_refused_claim_returns_no_timestamp() -> Result<()> {
    let storage = Arc::new(Storage {
        inner: acyclic_stream::MemoryStream::new_with_clock(
            Default::default(),
            Arc::new(ProviderClock),
        ),
        armed: AtomicBool::new(false),
        stalled: AtomicBool::new(false),
        observed: Mutex::new(None),
    });
    let (mut journal, _issuer) =
        test_support::journal_with_backends(storage, acyclic_fs::Fs::memory()).await?;
    journal.clock = Some(Arc::new(Clock));
    let operation = OperationId::new();
    journal
        .append(
            operation,
            "start",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let invocation = journal
        .stage_with_io(operation, "invocation", b"{}", "application/json", None)
        .await?;
    let competing = journal
        .stage_with_io(operation, "competing", b"{}", "application/json", None)
        .await?;
    journal
        .append(
            operation,
            "other",
            &ExecutionEvent::ToolStarted {
                step: 0,
                call_id: "other".into(),
                invocation: competing,
                publication: None,
                started_at_micros: None,
            },
            None,
        )?
        .await?;
    let mut event = ExecutionEvent::ToolStarted {
        step: 0,
        call_id: "claim".into(),
        invocation,
        publication: None,
        started_at_micros: None,
    };
    assert_eq!(
        journal
            .claim_if_tail(operation, 1, "claim", &mut event, None)?
            .await?,
        None,
        "a claim that lost the expected tail must not report a timestamp"
    );
    Ok(())
}
