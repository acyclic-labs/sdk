use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct HeldHead {
    inner: acyclic_stream::MemoryStream,
    release: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    release_read: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    heads: AtomicUsize,
    reads: AtomicUsize,
    follows: AtomicUsize,
    lose_follow: std::sync::atomic::AtomicBool,
    follow_loss: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

#[async_trait]
impl StreamProvider for HeldHead {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        self.inner.context()
    }
    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> Result<acyclic_stream::Heads, StreamError> {
        self.heads.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.heads(request).await?;
        let release = self.release.lock().unwrap().take();
        if let Some(release) = release {
            release.await.map_err(|_| StreamError::Unavailable)?;
        }
        Ok(result)
    }
    async fn durability_barrier(&self) -> Result<(), StreamError> {
        self.inner.durability_barrier().await
    }
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> acyclic_stream::BoxProviderFuture<'_, Result<acyclic_stream::WriteOutcome, StreamError>>
    {
        self.inner.write(request)
    }
    async fn read(
        &self,
        request: ReadRequest,
    ) -> Result<acyclic_stream::RecordStream, StreamError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.read(request).await?;
        let release = self.release_read.lock().unwrap().take();
        if let Some(release) = release {
            release.await.map_err(|_| StreamError::Unavailable)?;
        }
        Ok(result)
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> Result<acyclic_stream::FollowStream, StreamError> {
        self.follows.fetch_add(1, Ordering::Relaxed);
        if self.lose_follow.load(Ordering::Relaxed) {
            return Err(StreamError::Unavailable);
        }
        let original = self.inner.follow(request).await?;
        let loss = self.follow_loss.lock().unwrap().take();
        Ok(match loss {
            None => original,
            Some(loss) => original.map_cursor(|cursor| {
                stream::select(
                    cursor,
                    stream::once(async move {
                        let _ = loss.await;
                        Err(StreamError::Unavailable)
                    })
                    .boxed(),
                )
                .boxed()
            }),
        })
    }
    async fn read_commit(
        &self,
        commit: CommitId,
    ) -> Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.inner.read_commit(commit).await
    }
}

async fn original_journal()
-> Result<(MachineObservation, Arc<HeldHead>, Journal), Box<dyn std::error::Error>> {
    let MutationOutcome::Created(machine) = SimulatedMachines::default()
        .create(CreateMachine::new(
            IdempotencyKey::new(),
            Some(Image::custom([7; 32])?),
            [8; 32],
        ))
        .await?
    else {
        panic!("original create observation");
    };
    let streams = Arc::new(HeldHead::default());
    let journal = Journal::new(
        streams.clone(),
        StreamPath::new(format!("machine-tests/{}", machine.id))?,
        machine.id,
    );
    journal.enroll(machine.clone()).await?;
    Ok((machine, streams, journal))
}

#[tokio::test]
async fn machines_call_recovery_transfers_original_decoded_body()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut machine, _, journal) = original_journal().await?;
    // Vary the actual simulator outcome's endpoint before the original writer
    // commits it. Stay within the original Streams record boundary, including
    // the completion envelope. These size controls are not service captures.
    let endpoint = machine.endpoints[0].uri.clone();
    for bytes in [1 << 10, 1 << 13, acyclic_stream::MAX_RECORD_BYTES / 2] {
        machine.endpoints[0].uri = endpoint.repeat(bytes / endpoint.len() + 1);
        let key = IdempotencyKey::new();
        let Admission::Original(publication) =
            journal.begin(key, [9; 32], None, || Ok(|| {})).await?
        else {
            panic!("original completion must have its own admission");
        };
        journal
            .finish(
                key,
                [9; 32],
                publication,
                &machine,
                MutationOutcome::Created(machine.clone()),
            )
            .await?;
        // This is the same read/fold and owned-result handoff as recover and
        // duplicate begin. Retain the original allocation identity at that seam.
        let original = journal.existing_call(&journal.call(key)?).await?;
        let Some(MutationOutcome::Created(decoded)) = original.completed.as_ref() else {
            panic!("original committed completion must decode");
        };
        let endpoints = decoded.endpoints.as_ptr();
        let uri = decoded.endpoints[0].uri.as_ptr();
        let mut transferred = None;
        let handoff = acyclic_test_support::measure(|| transferred = Some(original.outcome(key)));
        let MutationOutcome::Created(recovered) = transferred.expect("measured handoff ran")?
        else {
            panic!("recovery must retain the original outcome");
        };
        assert_eq!(
            handoff.count_total, 0,
            "recovery handoff must not allocate or copy the original decoded body"
        );
        assert_eq!(handoff.bytes_total, 0);
        assert_eq!(
            recovered.endpoints.as_ptr(),
            endpoints,
            "recovery must transfer the original decoded result allocation"
        );
        assert_eq!(recovered.endpoints[0].uri.as_ptr(), uri);
        assert_eq!(recovered, machine);
        assert_eq!(
            journal.recover(key).await?,
            MutationOutcome::Created(machine.clone())
        );
        assert!(matches!(
            journal.begin(key, [9; 32], None, || panic!("accepted work cannot reenter")).await?,
            Admission::Replay(MutationOutcome::Created(replayed)) if replayed == machine
        ));
    }
    Ok(())
}

#[tokio::test]
async fn machines_call_event_follow_keeps_original_subscription_and_owner_loss()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut machine, streams, journal) = original_journal().await?;
    let heads = streams.heads.load(Ordering::Relaxed);
    let (release, released) = tokio::sync::oneshot::channel();
    *streams.release.lock().unwrap() = Some(released);
    let mut opening = Box::pin(journal.follow_events(Some(1)));
    assert!(futures::poll!(&mut opening).is_pending());
    assert_eq!(streams.heads.load(Ordering::Relaxed), heads + 1);

    // Publish through the original commit owner between captured head and follow.
    let key = IdempotencyKey::new();
    machine.state = MachineState::Destroying;
    let Admission::Original(publication) = journal
        .begin(key, [9; 32], Some(&machine), || Ok(|| {}))
        .await?
    else {
        panic!("original lifecycle admission");
    };
    machine.state = MachineState::Destroyed;
    journal
        .finish(
            key,
            [9; 32],
            publication,
            &machine,
            MutationOutcome::MachineDestroyed(machine.id),
        )
        .await?;
    release.send(()).unwrap();
    let mut events = opening.await?;
    for (sequence, state) in [(2, MachineState::Destroying), (3, MachineState::Destroyed)] {
        let event = events.next().await.unwrap()?;
        assert_eq!(event.machine, machine.id);
        assert_eq!(event.sequence, sequence);
        assert_eq!(event.fact, EventFact::State(state));
    }
    assert!(events.next().await.is_none());
    assert_eq!(streams.heads.load(Ordering::Relaxed), heads + 2);
    assert_eq!(streams.follows.load(Ordering::Relaxed), 1);
    assert!(matches!(
        journal.follow_events(Some(3)).await,
        Err(ProviderError::Conflict(_))
    ));
    assert!(matches!(
        journal.follow_events(Some(4)).await,
        Err(ProviderError::Invalid(_))
    ));

    machine.id = MachineId::new();
    machine.state = MachineState::Running;
    let original = Journal::new(
        streams.clone(),
        StreamPath::new(format!("machine-tests/{}", machine.id))?,
        machine.id,
    );
    original.enroll(machine.clone()).await?;
    streams.lose_follow.store(true, Ordering::Relaxed);
    assert!(matches!(
        original.follow_events(Some(1)).await,
        Err(ProviderError::Unavailable)
    ));
    streams.lose_follow.store(false, Ordering::Relaxed);
    let (source, loss) = tokio::sync::oneshot::channel();
    *streams.follow_loss.lock().unwrap() = Some(loss);
    let mut source_lost = original.follow_events(Some(1)).await?;
    assert!(futures::poll!(source_lost.next()).is_pending());
    drop(source);
    assert!(matches!(
        source_lost.next().await,
        Some(Err(ProviderError::Unavailable))
    ));
    assert!(source_lost.next().await.is_none());
    let mut lost = original.follow_events(Some(1)).await?;
    assert!(futures::poll!(lost.next()).is_pending());
    drop(original);
    assert!(matches!(
        futures::poll!(lost.next()),
        std::task::Poll::Ready(Some(Err(ProviderError::Unavailable)))
    ));
    assert!(lost.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn machines_call_held_read_keeps_admission_and_original_terminal_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut machine, streams, journal) = original_journal().await?;
    let original = IdempotencyKey::new();
    machine.state = MachineState::Suspending;
    let Admission::Original(publication) = journal
        .begin(original, [9; 32], Some(&machine), || Ok(|| {}))
        .await?
    else {
        panic!("original admission must retain its publication");
    };

    let reads = streams.reads.load(Ordering::Relaxed);
    let (release, released) = tokio::sync::oneshot::channel();
    *streams.release_read.lock().unwrap() = Some(released);
    let mut opening = Box::pin(journal.watch(OperationId(original.0)));
    // Hold the original finite read, before opening its successor follow.
    assert!(futures::poll!(&mut opening).is_pending());
    assert_eq!(streams.reads.load(Ordering::Relaxed), reads + 1);
    machine.state = MachineState::Suspended;
    journal
        .finish(
            original,
            [9; 32],
            publication,
            &machine,
            MutationOutcome::Suspended(machine.id),
        )
        .await?;

    let later = IdempotencyKey::new();
    machine.state = MachineState::Waking;
    let mut admission = Box::pin(journal.begin(later, [10; 32], Some(&machine), || Ok(|| {})));
    // The former lock across the held head fails this assertion immediately.
    let std::task::Poll::Ready(Ok(Admission::Original(publication))) =
        futures::poll!(&mut admission)
    else {
        panic!("a call read must not retain the lifecycle admission lock");
    };
    drop(admission);
    machine.state = MachineState::Running;
    journal
        .finish(
            later,
            [10; 32],
            publication,
            &machine,
            MutationOutcome::Woken(machine.id),
        )
        .await?;
    release.send(()).unwrap();

    let mut watched = opening.await?;
    for phase in [OperationPhase::Pending, OperationPhase::Succeeded] {
        assert_eq!(
            watched.next().await.unwrap()?,
            OperationObservation {
                id: OperationId(original.0),
                phase,
            }
        );
    }
    assert!(watched.next().await.is_none());
    assert_eq!(streams.reads.load(Ordering::Relaxed), reads + 2);
    assert_eq!(streams.follows.load(Ordering::Relaxed), 1);
    assert_eq!(
        journal.recover(original).await?,
        MutationOutcome::Suspended(machine.id)
    );
    assert_eq!(
        journal.recover(later).await?,
        MutationOutcome::Woken(machine.id)
    );

    // The original reader can stall before its successor follow is installed.
    // Losing the retained producer must end that wait, without releasing IO.
    let lost = IdempotencyKey::new();
    let Admission::Original(Some(publication)) = journal
        .begin(lost, [11; 32], Some(&machine), || Ok(|| {}))
        .await?
    else {
        panic!("original held-read acceptance");
    };
    let (release, released) = tokio::sync::oneshot::channel();
    *streams.release_read.lock().unwrap() = Some(released);
    let mut opening = Box::pin(journal.watch(OperationId(lost.0)));
    assert!(futures::poll!(&mut opening).is_pending());
    drop(publication);
    assert!(
        matches!(futures::poll!(&mut opening), std::task::Poll::Ready(Err(ProviderError::Indeterminate(key))) if key == lost),
        "a held original read must observe its producer loss"
    );
    drop(opening);
    drop(release);
    assert_eq!(streams.follows.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn machines_call_abandoned_preparation_keeps_unsubmitted_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let (machine, _, journal) = original_journal().await?;
    let mut pending = machine.clone();
    pending.state = MachineState::Suspending;
    let state = std::cell::Cell::new(machine.state);
    let key = IdempotencyKey::new();
    let digest = [9; 32];
    // Withhold the actual preparation lock. This is before the original
    // Streams submission, so abandoning this wait must preserve availability.
    let preparation = journal.producer.lock().await;
    let mut operation = Box::pin(journal.begin(key, digest, Some(&pending), || {
        Ok(|| state.set(pending.state))
    }));
    assert!(futures::poll!(&mut operation).is_pending());
    assert_eq!(state.get(), MachineState::Running);
    drop(operation);
    drop(preparation);
    assert_eq!(journal.machine().await?, machine);
    assert!(matches!(
        journal.recover(key).await,
        Err(ProviderError::NotFound(_))
    ));

    // No mutation was submitted: the same original key can still make progress.
    let Admission::Original(publication) = journal
        .begin(key, digest, Some(&pending), || {
            Ok(|| state.set(pending.state))
        })
        .await?
    else {
        panic!("unsubmitted preparation cannot create a replay");
    };
    assert_eq!(state.get(), MachineState::Suspending);
    pending.state = MachineState::Suspended;
    let outcome = MutationOutcome::Suspended(machine.id);
    assert_eq!(
        journal
            .finish(key, digest, publication, &pending, outcome.clone())
            .await?,
        outcome
    );
    assert_eq!(journal.recover(key).await?, outcome);
    Ok(())
}

#[tokio::test]
async fn machines_call_has_one_terminal_and_never_reenters_accepted_work()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut machine, streams, journal) = original_journal().await?;
    let machine_path = journal.path(format_args!("machines/{}", machine.id))?;
    let entered = std::cell::Cell::new(0);
    let key = IdempotencyKey::new();
    let digest = [9; 32];
    machine.state = MachineState::Suspending;
    let Admission::Original(Some(publication)) = journal
        .begin(key, digest, Some(&machine), || {
            entered.set(entered.get() + 1);
            Ok(|| {})
        })
        .await?
    else {
        panic!("original physical acceptance");
    };
    let path = journal.call(key)?;
    let accepted = streams.head(path.log().clone()).await?;
    let pending = streams.head(machine_path.clone()).await?;
    assert_eq!(accepted.tail, 1);
    assert_eq!(
        accepted.last.as_ref().unwrap().commit_id,
        publication.commit
    );
    assert_eq!(pending.last.as_ref().unwrap().commit_id, publication.commit);
    machine.state = MachineState::Suspended;
    let outcome = MutationOutcome::Suspended(machine.id);
    journal
        .finish(key, digest, Some(publication), &machine, outcome.clone())
        .await?;
    let settled = streams.head(path.log().clone()).await?;
    let marker = streams.head(path.settled().clone()).await?;
    let final_machine = streams.head(machine_path.clone()).await?;
    assert_eq!(settled.tail, 2);
    assert_eq!(marker.tail, 1);
    let terminal = settled.last.as_ref().unwrap().commit_id;
    assert_eq!(marker.last.as_ref().unwrap().commit_id, terminal);
    assert_eq!(final_machine.last.as_ref().unwrap().commit_id, terminal);
    assert_eq!(final_machine.tail, pending.tail + 1);
    assert!(matches!(journal.begin(key, digest, Some(&machine), || {
        entered.set(entered.get() + 1);
        Ok(|| {})
    }).await?, Admission::Replay(replayed) if replayed == outcome));
    assert!(matches!(
        journal
            .begin(key, [10; 32], Some(&machine), || {
                entered.set(entered.get() + 1);
                Ok(|| {})
            })
            .await,
        Err(ProviderError::Conflict(_))
    ));
    assert_eq!(entered.get(), 1);

    // A logical no-op needs only the terminal commit, joined with its state.
    let logical = IdempotencyKey::new();
    assert!(matches!(
        journal.begin(logical, digest, None, || Ok(|| {})).await?,
        Admission::Original(None)
    ));
    journal
        .finish(logical, digest, None, &machine, outcome)
        .await?;
    let logical_path = journal.call(logical)?;
    assert_eq!(streams.head(logical_path.log().clone()).await?.tail, 1);
    assert_eq!(
        journal.inspect(OperationId(logical.0)).await?.phase,
        OperationPhase::Succeeded
    );
    Ok(())
}

#[tokio::test]
async fn machines_call_cancel_and_lost_producer_preserve_original_uncertainty()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut machine, streams, journal) = original_journal().await?;
    let machine_path = journal.path(format_args!("machines/{}", machine.id))?;
    let digest = [9; 32];
    let entered = std::cell::Cell::new(0);

    // Cancellation commits while the original physical producer is retained.
    let cancelled = IdempotencyKey::new();
    machine.state = MachineState::Waking;
    let Admission::Original(Some(publication)) = journal
        .begin(cancelled, digest, Some(&machine), || Ok(|| {}))
        .await?
    else {
        panic!("original cancellation acceptance");
    };
    let original_pending = streams.head(machine_path.clone()).await?;
    let mut watched = journal.watch(OperationId(cancelled.0)).await?;
    assert_eq!(
        watched.next().await.unwrap()?.phase,
        OperationPhase::Pending
    );
    let preparation = journal.producer.lock().await;
    let mut cancel = Box::pin(journal.cancel(OperationId(cancelled.0)));
    assert!(matches!(
        futures::poll!(&mut cancel),
        std::task::Poll::Ready(Ok(OperationObservation {
            phase: OperationPhase::Indeterminate,
            ..
        }))
    ));
    drop(cancel);
    drop(preparation);
    assert_eq!(
        watched.next().await.unwrap()?.phase,
        OperationPhase::Indeterminate
    );
    assert!(watched.next().await.is_none());
    machine.state = MachineState::Running;
    assert_eq!(
        journal
            .finish(
                cancelled,
                digest,
                Some(publication),
                &machine,
                MutationOutcome::Woken(machine.id)
            )
            .await,
        Err(ProviderError::Indeterminate(cancelled))
    );
    assert_eq!(streams.head(machine_path.clone()).await?, original_pending);
    let cancelled_path = journal.call(cancelled)?;
    assert_eq!(streams.head(cancelled_path.log().clone()).await?.tail, 2);
    assert_eq!(
        streams.head(cancelled_path.settled().clone()).await?.tail,
        1
    );
    assert_eq!(
        journal.recover(cancelled).await,
        Err(ProviderError::Indeterminate(cancelled))
    );

    // Lost custody ends the original wait and leaves acceptance unreplayable.
    let lost = IdempotencyKey::new();
    let Admission::Original(Some(publication)) = journal
        .begin(lost, digest, Some(&machine), || Ok(|| {}))
        .await?
    else {
        panic!("original lost-producer acceptance");
    };
    let mut watched = journal.watch(OperationId(lost.0)).await?;
    assert_eq!(
        watched.next().await.unwrap()?.phase,
        OperationPhase::Pending
    );
    assert!(futures::poll!(watched.next()).is_pending());
    drop(publication);
    assert!(
        matches!(futures::poll!(watched.next()), std::task::Poll::Ready(Some(Err(ProviderError::Indeterminate(key)))) if key == lost)
    );
    assert!(watched.next().await.is_none());
    assert!(matches!(journal.begin(lost, digest, Some(&machine), || {
        entered.set(entered.get() + 1);
        Ok(|| {})
    }).await, Err(ProviderError::Indeterminate(key)) if key == lost));
    assert_eq!(entered.get(), 0);
    assert_eq!(
        streams.head(journal.call(lost)?.log().clone()).await?.tail,
        1
    );

    // A gate can settle the SAME original call before physical acceptance.
    let denied = IdempotencyKey::new();
    let denied_path = journal.call(denied)?;
    assert!(matches!(
        streams
            .commit(denied_path.cancel("original caller revoked")?.request(
                acyclic_stream::IdempotencyKey::new(denied.as_bytes().to_vec())?
            ))
            .await?,
        CommitOutcome::Committed(_)
    ));
    assert!(matches!(
        journal
            .begin(denied, digest, Some(&machine), || {
                entered.set(entered.get() + 1);
                Ok(|| {})
            })
            .await,
        Err(ProviderError::Cancelled)
    ));
    assert_eq!(entered.get(), 0);
    assert!(matches!(
        journal.cancel(OperationId::new()).await,
        Err(ProviderError::NotFound(_))
    ));
    Ok(())
}
