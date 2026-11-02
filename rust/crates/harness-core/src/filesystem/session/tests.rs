use super::*;
use crate::executor::{ExecutionAttempt, ExecutionClaims};
use acyclic_fs::WorkBudget;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Wake;

#[derive(Default)]
struct ParentWake(AtomicUsize);

impl Wake for ParentWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn serving_owner_drives_notified_producers_and_retires_exact_claims() -> Result<()> {
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: 8 * 1024 * 1024,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = original.live_allocation_bytes();
    let (mut owner, session) = SessionWork::admit(original.clone())?;
    let selected = session.agent(&AgentId::from_bytes([1; 16]))?;
    let parent = Arc::new(ParentWake::default());
    let waker = Waker::from(Arc::clone(&parent));
    let mut context = Context::from_waker(&waker);
    assert!(owner.poll(&mut context).is_pending());

    let entered = Arc::new(AtomicBool::new(false));
    let built = Arc::clone(&entered);
    let remaining = original.remaining()?.peak_allocation_bytes;
    let exhausted = original.claim(remaining, 1, WorkCounters::UNCHARGED)?;
    assert!(
        selected
            .attach(move |_| {
                built.store(true, Ordering::SeqCst);
                async {}
            })
            .is_err()
    );
    assert!(!entered.load(Ordering::SeqCst));
    drop(exhausted);

    // Handoff is FIFO even when both producers arrive before owner delivery.
    // Removing from the back would strand the oldest notification here.
    let (first, mut first_result) = tokio::sync::oneshot::channel();
    let (second, mut second_result) = tokio::sync::oneshot::channel();
    selected.attach(move |_| async move {
        first.send(()).unwrap();
    })?;
    selected.attach(move |_| async move {
        second.send(()).unwrap();
    })?;
    assert!(owner.poll(&mut context).is_pending());
    assert_eq!(first_result.try_recv(), Ok(()));
    assert!(matches!(
        second_result.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    assert!(owner.poll(&mut context).is_pending());
    assert_eq!(second_result.try_recv(), Ok(()));

    let claims = Arc::new(ExecutionClaims::default());
    let polls = Arc::new(AtomicUsize::new(0));
    let (finish, mut completion) = tokio::sync::oneshot::channel();
    let (started, observed) = tokio::sync::oneshot::channel();
    let held_claims = Arc::clone(&claims);
    let held_polls = Arc::clone(&polls);
    let before = parent.0.load(Ordering::SeqCst);
    selected.attach(move |slot| async move {
        let ExecutionAttempt::Owned(claim) = held_claims
            .acquire(
                [7; 32],
                crate::OperationId::from_bytes([1; 16]),
                Some(slot.preparation().unwrap()),
            )
            .unwrap()
        else {
            panic!("original producer already exists")
        };
        started.send(claim.observe()).ok().unwrap();
        std::future::poll_fn(|context| {
            held_polls.fetch_add(1, Ordering::SeqCst);
            std::pin::Pin::new(&mut completion).poll(context)
        })
        .await
        .unwrap();
        claim.release();
    })?;
    assert!(parent.0.load(Ordering::SeqCst) > before);
    assert!(owner.poll(&mut context).is_pending());
    let observed = observed.await.unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    // With no producer event, polling the serving loop cannot poll that child.
    assert!(owner.poll(&mut context).is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 1);

    // A slow earlier producer must not retain completed slots from other
    // agents. Restoring issue-ordered retirement fails the first active count.
    let other = session.agent(&AgentId::from_bytes([2; 16]))?;
    let mut resident = None;
    for _ in 0..64 {
        let (finished, result) = tokio::sync::oneshot::channel();
        other.attach(move |_| async move {
            finished.send(()).unwrap();
        })?;
        assert!(owner.poll(&mut context).is_pending());
        result.await.unwrap();
        assert_eq!(
            owner.futures.len(),
            1,
            "completed producers retained behind a slow agent"
        );
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        match resident {
            None => resident = Some(original.live_allocation_bytes()),
            Some(bytes) => assert_eq!(original.live_allocation_bytes(), bytes),
        }
    }
    finish.send(()).unwrap();
    assert!(owner.poll(&mut context).is_pending());
    assert_eq!(
        observed.terminated().await,
        acyclic_fs::OperationTermination::Released
    );
    assert!(owner.futures.is_empty());

    // Cancellation before the first owner poll destroys the original admitted
    // capture without invoking its builder; no detached producer survives.
    let entered = Arc::new(AtomicBool::new(false));
    let built = Arc::clone(&entered);
    let (unpolled, lost) = tokio::sync::oneshot::channel::<()>();
    selected.attach(move |_| {
        built.store(true, Ordering::SeqCst);
        async move {
            drop(unpolled);
        }
    })?;
    drop(owner);
    assert!(lost.await.is_err());
    assert!(!entered.load(Ordering::SeqCst));
    assert!(selected.attach(|_| async {}).is_err());
    drop((observed, other, selected, session, claims));
    assert_eq!(original.live_allocation_bytes(), baseline);

    // A producer already suspended on its original event releases exclusion as
    // ProducerDropped on owner loss, never as a successful committed result.
    let (mut owner, session) = SessionWork::admit(original.clone())?;
    let selected = session.agent(&AgentId::from_bytes([1; 16]))?;
    let claims = Arc::new(ExecutionClaims::default());
    let (started, observed) = tokio::sync::oneshot::channel();
    selected.attach(move |slot| async move {
        let claim = claims
            .acquire(
                [8; 32],
                crate::OperationId::from_bytes([2; 16]),
                Some(slot.preparation().unwrap()),
            )
            .unwrap()
            .into_owner(Some(slot.preparation().unwrap()))
            .unwrap();
        started.send(claim.observe()).ok().unwrap();
        std::future::pending::<()>().await;
        claim.release();
    })?;
    assert!(owner.poll(&mut context).is_pending());
    let observed = observed.await.unwrap();
    drop(owner);
    assert_eq!(
        observed.terminated().await,
        acyclic_fs::OperationTermination::ProducerDropped
    );
    drop((observed, selected, session));
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn retained_agent_slots_share_admission_and_stop_with_the_runtime_owner() -> Result<()> {
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: 8 * 1024 * 1024,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = original.live_allocation_bytes();
    let (owner, session) = SessionWork::admit(original.clone())?;
    let binding = SessionBinding::default();
    binding.admit(&session)?;
    binding.admit(&session)?;
    // Runtime startup admits its own original work. A competing runtime has
    // independent admission before testing the executor's live-owner exclusion.
    let replacement = NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: 8 * 1024 * 1024,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let replacement_baseline = replacement.live_allocation_bytes();
    let (successor, next) = SessionWork::admit(replacement.clone())?;
    // A fresh runtime must not displace the original live producer.
    assert!(binding.admit(&next).is_err());
    assert!(Arc::ptr_eq(&binding.get()?.unwrap(), &session));
    let id = AgentId::from_bytes([1; 16]);
    let selected = session.agent(&id)?;
    let repeat = session.agent(&id)?;
    assert!(Arc::ptr_eq(&selected, &repeat));
    assert!(
        selected
            .preparation()?
            .original_native_work()
            .unwrap()
            .shares_ledger(&original)
    );

    // A real turn scope retires without closing the retained runtime admission.
    {
        let (turn, turn_cancellation) = original.invocation_view()?;
        let turn_io = PreparationIo::from_native_work(&turn)?;
        turn_cancellation.cancel();
        drop((turn_io, turn, turn_cancellation));
    }
    selected.preparation()?;
    assert!(binding.admit(&next).is_err());

    for number in 2_u128..1026 {
        session.agent(&AgentId::from_bytes(number.to_be_bytes()))?;
    }
    let before = original.receipt();
    assert!(Arc::ptr_eq(&selected, &session.agent(&id)?));
    let after = original.receipt();
    assert_eq!(after.allocation_operations, before.allocation_operations);
    assert!(after.items_examined - before.items_examined <= 2 * (128 + 16));

    {
        let work = selected.preparation()?.original_native_work().unwrap();
        let stopped = work.cancellation_wait()?;
        futures::pin_mut!(stopped);
        assert!(futures::poll!(&mut stopped).is_pending());
        drop(owner);
        assert!(futures::poll!(&mut stopped).is_ready());
    }
    assert!(selected.preparation().is_err());
    assert!(session.agent(&id).is_err());
    // Closing one runtime does not cancel its caller or a later runtime.
    original.remaining()?;
    // Replacement depends on the original owner-loss event, not a failed
    // allocation, turn cancellation or a new runtime's mere existence.
    binding.admit(&next)?;
    binding.admit(&next)?;
    assert!(Arc::ptr_eq(&binding.get()?.unwrap(), &next));
    assert!(binding.admit(&session).is_err());
    next.agent(&id)?.preparation()?;
    drop(successor);
    assert!(binding.admit(&next).is_err());
    drop((binding, next, repeat));
    drop(selected);
    drop(session);
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(replacement.live_allocation_bytes(), replacement_baseline);
    Ok(())
}
