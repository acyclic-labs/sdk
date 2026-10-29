use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct HeldHead {
    inner: acyclic_stream::MemoryStream,
    release: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    heads: AtomicUsize,
    follows: AtomicUsize,
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
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> Result<acyclic_stream::FollowStream, StreamError> {
        self.follows.fetch_add(1, Ordering::Relaxed);
        self.inner.follow(request).await
    }
    async fn read_commit(
        &self,
        commit: CommitId,
    ) -> Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.inner.read_commit(commit).await
    }
}

#[tokio::test]
async fn held_watch_head_keeps_lifecycle_admission_and_original_terminal_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let MutationOutcome::Created(mut machine) = SimulatedMachines::default()
        .create(CreateMachine::new(
            IdempotencyKey::new(),
            Some(Image::custom([7; 32])?),
            [8; 32],
        ))
        .await?
    else {
        panic!("create must return its original observation");
    };
    let streams = Arc::new(HeldHead::default());
    let journal = Journal::new(
        streams.clone(),
        StreamPath::new(format!("machine-tests/{}", machine.id))?,
        machine.id,
    );
    journal.enroll(machine.clone()).await?;
    let original = IdempotencyKey::new();
    machine.state = MachineState::Suspending;
    let Admission::Original(publication) = journal
        .begin(original, [9; 32], Some(&machine), || Ok(|| {}))
        .await?
    else {
        panic!("original admission must retain its publication");
    };

    let heads = streams.heads.load(Ordering::Relaxed);
    let (release, released) = tokio::sync::oneshot::channel();
    *streams.release.lock().unwrap() = Some(released);
    let mut opening = Box::pin(journal.watch(OperationId(original.0)));
    // Hold the real captured pending head, not a fabricated operation response.
    assert!(futures::poll!(&mut opening).is_pending());
    assert_eq!(streams.heads.load(Ordering::Relaxed), heads + 1);
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
        panic!("a watch head must not retain the lifecycle admission lock");
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
    assert_eq!(streams.heads.load(Ordering::Relaxed), heads + 3);
    assert_eq!(streams.follows.load(Ordering::Relaxed), 1);
    assert_eq!(
        journal.recover(original).await?,
        MutationOutcome::Suspended(machine.id)
    );
    assert_eq!(
        journal.recover(later).await?,
        MutationOutcome::Woken(machine.id)
    );
    Ok(())
}

#[tokio::test]
async fn abandoned_preparation_does_not_close_unsubmitted_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let MutationOutcome::Created(machine) = SimulatedMachines::default()
        .create(CreateMachine::new(
            IdempotencyKey::new(),
            Some(Image::custom([7; 32])?),
            [8; 32],
        ))
        .await?
    else {
        panic!("create must return its original observation");
    };
    let journal = Journal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        StreamPath::new(format!("machine-tests/{}", machine.id))?,
        machine.id,
    );
    journal.enroll(machine.clone()).await?;
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
