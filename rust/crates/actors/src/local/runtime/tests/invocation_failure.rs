use super::*;
use crate::guest::{CancellationToken, GuestError};
use crate::invocation::Invocation;
use futures::TryStreamExt as _;

struct Rejects {
    package: Package,
    calls: AtomicUsize,
}

fn cause() -> GuestError {
    GuestError::Decode("original deterministic guest failure".into())
}

impl Handler for Rejects {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(invocation) = delivery.invocation()? {
                if invocation.request().as_ref() == b"cancel" {
                    cancellation.cancel();
                    return Err(cause());
                }
                if invocation.request().as_ref() == b"fail" {
                    return Err(cause());
                }
            }
            self.package
                .on_record(delivery, checkpoint, cancellation)
                .await
        })
    }
}

async fn terminal_cut<P: StreamProvider>(provider: Arc<Counted<P>>) -> TestResult {
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = actor("terminal-failure")?;
    let input = StreamPath::new("terminal-failure/inbox")?;
    let checkpoint = ledger("terminal-failure/outbox")?;
    let mut create = PreparedPublish::foreign(key("terminal-failure/create")?);
    for effect in store.create(
        &address,
        BTreeMap::from([(
            "inbox".into(),
            Subscription {
                path: input,
                cursor: 0,
            },
        )]),
        Some(checkpoint.clone()),
    )? {
        create.join(effect)?;
    }
    store.submit(create).await?;
    let handler = Arc::new(Rejects {
        package: Package::new()?,
        calls: AtomicUsize::new(0),
    });
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        handler.clone(),
        Arc::new(SystemClock),
    );
    runtime.track(address.clone())?;
    runtime.drain_actor(&address).await?; // An idle actor does not claim placement.
    let invocation = Invocation::new(
        acyclic_stream::StreamPath::new("invoke-control/caller")?,
        address.clone(),
        "inbox".into(),
        Bytes::from_static(b"failed"),
        Bytes::from_static(b"fail"),
    )?;
    let ticket = store.enqueue_invoke(&invocation).await?;
    let snapshot = store.load(&address).await?;
    let epoch = snapshot.state().ok_or("missing original state")?.epoch();
    let wrong = Trigger::Record {
        subscription: "inbox".into(),
        sequence: ticket.record().sequence + 1,
    };
    assert!(
        store
            .prepare_invocation_failure(
                &snapshot,
                epoch,
                &wrong,
                Some(ticket.record()),
                &cause(),
                key("terminal-failure/wrong")?
            )?
            .is_none()
    );
    assert!(matches!(
        store.prepare_invocation_failure(
            &snapshot,
            epoch + 1,
            &Trigger::Record {
                subscription: "inbox".into(),
                sequence: ticket.record().sequence
            },
            Some(ticket.record()),
            &cause(),
            key("terminal-failure/epoch")?
        ),
        Err(Error::Conflict)
    ));
    let (failed, drained) = futures::join!(
        store.wait_invocation(ticket.clone()),
        runtime.drain_actor(&address)
    );
    drained?;
    let Err(Error::InvocationFailed { source, receipt }) = failed else {
        return Err("failure did not settle invocation".into());
    };
    assert_eq!(source, cause());
    assert_eq!(receipt.ticket(), &ticket);
    assert_eq!(receipt.state().commit_id, receipt.terminal().commit_id);
    let mut publications = provider
        .read(acyclic_stream::ReadRequest {
            key: None,
            path: snapshot.path().clone(),
            from: snapshot.tail,
            limit: 2,
            at_least: Default::default(),
        })
        .await?;
    let claim = publications
        .try_next()
        .await?
        .ok_or("missing placement claim")?;
    let claimed = State::decode(&claim.value)?;
    assert_eq!(claimed.epoch(), epoch + 1);
    assert_eq!(claimed.cursor("inbox"), Some(0));
    assert_eq!(claimed.checkpoint(), Some(&checkpoint));
    assert_ne!(claim.commit_id, receipt.state().commit_id);
    assert_eq!(
        publications.try_next().await?.as_ref(),
        Some(receipt.state())
    );
    assert!(publications.try_next().await?.is_none());
    let retained = store.load(&address).await?;
    assert_eq!(
        retained.tail - claim.sequence - 1,
        1,
        "failure consumes input and replies in one state publication"
    );
    let state = retained.state().ok_or("missing terminal state")?;
    assert_eq!(state.checkpoint(), Some(&checkpoint));
    assert_eq!(state.cursor("inbox"), Some(1));
    assert_eq!(
        state.alarm(),
        snapshot.state().ok_or("missing original state")?.alarm()
    );
    let reopened = Store::new(Arc::clone(&provider), root()?);
    match reopened.invoke(&invocation).await {
        Err(Error::InvocationFailed {
            source: repeated,
            receipt: original,
        }) => {
            assert_eq!(repeated, source);
            assert_eq!(original, receipt);
        }
        _ => return Err("reconciliation did not retain the original failure".into()),
    }
    runtime.drain_actor(&address).await?;
    assert_eq!(
        handler.calls.load(Ordering::SeqCst),
        1,
        "terminal failure cannot re-enter the guest"
    );

    let cancelled = Invocation::new(
        acyclic_stream::StreamPath::new("invoke-control/caller")?,
        address.clone(),
        "inbox".into(),
        Bytes::from_static(b"cancelled"),
        Bytes::from_static(b"cancel"),
    )?;
    let cancelled = store.enqueue_invoke(&cancelled).await?;
    assert!(
        matches!(runtime.drain_actor(&address).await, Err(Error::Handler(error)) if error == cause())
    );
    let after_cancel = store.load(&address).await?;
    assert_eq!(
        after_cancel.tail, retained.tail,
        "cancelled attempts cannot publish a terminal failure"
    );
    assert_eq!(
        after_cancel.state().and_then(|state| state.cursor("inbox")),
        Some(1)
    );
    let mut wait = Box::pin(store.wait_invocation(cancelled));
    assert!(futures::poll!(wait.as_mut()).is_pending());
    drop(wait);

    // The same inline executor must retire a cancelled call and run its next
    // input. Its speculative response, checkpoint and output never commit.
    let cancelled_actor = actor("inline-cancel")?;
    let input = StreamPath::new("inline-cancel/inbox")?;
    let output = StreamPath::new("inline-cancel/outbox")?;
    let mut create = PreparedPublish::foreign(key("inline-cancel/create")?);
    for effect in store.create(
        &cancelled_actor,
        BTreeMap::from([(
            "inbox".into(),
            Subscription {
                path: input,
                cursor: 0,
            },
        )]),
        Some(ledger(output.as_str())?),
    )? {
        create.join(effect)?;
    }
    store.submit(create).await?;
    runtime.track(cancelled_actor.clone())?;
    let caller = StreamPath::new("inline-cancel/caller")?;
    let request = |id: &'static [u8]| {
        Invocation::new(
            caller.clone(),
            cancelled_actor.clone(),
            "inbox".into(),
            Bytes::from_static(id),
            Bytes::from_static(b"emit"),
        )
    };
    let cancelled = request(b"cancelled")?;
    let ticket = store.enqueue_invoke(&cancelled).await?;
    let call = ticket.call()?;
    let cancel = call
        .cancel("original caller cancelled")?
        .request(key("inline-cancel/cancel")?);
    assert!(matches!(
        provider.commit(cancel).await?,
        acyclic_stream::CommitOutcome::Committed(_)
    ));
    let Err(Error::InvocationTerminated { receipt }) = store.wait_invocation(ticket).await else {
        return Err("call cancellation did not terminate the invocation observer".into());
    };
    assert!(matches!(
        receipt.outcome,
        crate::call::Outcome::Cancelled(_)
    ));
    let next = store.enqueue_invoke(&request(b"next")?).await?;
    runtime.drain_actor(&cancelled_actor).await?;
    let completed = store.wait_invocation(next).await?;
    assert!(matches!(
        completed.receipt().call().outcome,
        crate::call::Outcome::Ok(_)
    ));
    assert_eq!(
        provider.tail(output).await?,
        1,
        "cancelled inline effects cannot escape"
    );
    assert_eq!(
        store
            .load(&cancelled_actor)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(2)
    );
    Ok(())
}

/// Both primitive providers run the same terminal-reply contract. The shared
/// suite additionally rejects an adapter that converts failure into success.
#[tokio::test]
async fn invocation_failures_settle_with_original_cause_and_cut() -> TestResult {
    terminal_cut(Arc::new(Counted::new(MemoryStream::default()))).await?;
    let directory = tempfile::tempdir()?;
    terminal_cut(Arc::new(Counted::new(
        LocalStream::open(directory.path(), Default::default()).await?,
    )))
    .await?;
    let memory = memory_backend().await?;
    conformance::invocation_failure(&memory, "failure-memory").await?;
    let durable = durable_backend().await?;
    conformance::invocation_failure(&durable, "failure-durable").await?;
    let provider = Counted::new(MemoryStream::default());
    let requests = provider.ledger();
    let broken = LocalTarget::start(
        &Arc::new(provider),
        Arc::new(HidesFailure(Package::new()?)),
        requests,
    )?;
    assert!(matches!(
        conformance::invocation_failure(&broken, "failure-negative").await,
        Err(Failure::Violation {
            case: "invocation-failure",
            ..
        })
    ));
    Ok(())
}

struct HidesFailure(Package);
impl Handler for HidesFailure {
    fn manifest(&self) -> &Manifest {
        self.0.manifest()
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            match self.0.on_record(delivery, checkpoint, cancellation).await {
                Ok(transition) => Ok(transition),
                Err(error) => Ok(Transition {
                    response: Some(Bytes::from(error.to_string())),
                    checkpoint: checkpoint
                        .ok_or_else(|| {
                            GuestError::Handler("missing negative control checkpoint".into())
                        })?
                        .clone(),
                    alarm: AlarmChange::Keep,
                    effects: Vec::new(),
                    participant: None,
                }),
            }
        })
    }
}
