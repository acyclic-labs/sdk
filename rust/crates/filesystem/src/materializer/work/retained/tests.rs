use super::*;
use std::{
    cell::Cell,
    sync::atomic::{AtomicUsize, Ordering},
    task::Wake,
};

thread_local! {
    pub(super) static OMIT_WAKE: Cell<bool> = const { Cell::new(false) };
}

#[derive(Default)]
struct Notifications(AtomicUsize);
impl Wake for Notifications {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
impl Notifications {
    fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

fn original() -> (NativeMaterializationWork, crate::CancellationToken) {
    let cancellation = crate::CancellationToken::new();
    let budget = crate::WorkBudget {
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 65536,
        ..crate::WorkBudget::UNBOUNDED
    };
    let work = NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)
        .unwrap();
    (work, cancellation)
}

fn poll(
    claim: &mut NativeRetainedClaim,
    wake: &Arc<Notifications>,
) -> Poll<Result<NativeAllocation, NativeTreeMaterializationError>> {
    let waker = Waker::from(wake.clone());
    Pin::new(claim).poll(&mut Context::from_waker(&waker))
}

fn unavailable(result: Poll<Result<NativeAllocation, NativeTreeMaterializationError>>) -> bool {
    match result {
        Poll::Ready(Err(NativeTreeMaterializationError::Io(error))) => matches!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<crate::DemandError>()),
            Some(crate::DemandError::NativeWorkUnavailable)
        ),
        _ => false,
    }
}

// The same event oracle is run against the real release and a mutation which
// drops its selected wake. No timeout or second readiness probe hides the loss.
fn release_wakes_head(omit_wake: bool) -> bool {
    let (work, _) = original();
    let producer = work.retained_producer().unwrap();
    let blocker = work.claim(49152, 1, WorkCounters::UNCHARGED).unwrap();
    let mut first = work
        .claim_when_released(49152, 1, WorkCounters::UNCHARGED)
        .unwrap();
    let mut second = work
        .claim_when_released(49152, 1, WorkCounters::UNCHARGED)
        .unwrap();
    let first_wake = Arc::new(Notifications::default());
    let second_wake = Arc::new(Notifications::default());
    assert!(poll(&mut first, &first_wake).is_pending());
    assert!(poll(&mut second, &second_wake).is_pending());
    assert_eq!((first_wake.count(), second_wake.count()), (0, 0));
    OMIT_WAKE.with(|value| value.set(omit_wake));
    drop(blocker);
    OMIT_WAKE.with(|value| value.set(false));
    let woke_exact_head = (first_wake.count(), second_wake.count()) == (1, 0);
    // The first grant already owns its exact capacity before any continuation.
    assert!(work.claim(49152, 1, WorkCounters::UNCHARGED).is_err());
    // Cancellation of a granted future returns its original allocation and
    // hands the credit to its FIFO successor through the same release event.
    drop(first);
    assert_eq!(second_wake.count(), 1);
    let allocation = match poll(&mut second, &second_wake) {
        Poll::Ready(Ok(allocation)) => allocation,
        other => panic!("released original credit did not advance successor: {other:?}"),
    };
    drop((allocation, second, producer));
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    woke_exact_head
}

#[test]
fn retained_release_reserves_fifo_grant_and_cancellation_passes_baton() {
    assert!(release_wakes_head(false));
    assert!(!release_wakes_head(true));
}

#[test]
fn retained_producer_loss_terminates_exact_generation_without_canceling_successor() {
    let (work, _) = original();
    let producer = work.retained_producer().unwrap();
    let blocker = work.claim(49152, 1, WorkCounters::UNCHARGED).unwrap();
    let mut first = work
        .claim_when_released(49152, 1, WorkCounters::UNCHARGED)
        .unwrap();
    let mut second = work
        .claim_when_released(49152, 1, WorkCounters::UNCHARGED)
        .unwrap();
    let first_wake = Arc::new(Notifications::default());
    let second_wake = Arc::new(Notifications::default());
    assert!(poll(&mut first, &first_wake).is_pending());
    assert!(poll(&mut second, &second_wake).is_pending());
    drop(producer);
    assert_eq!((first_wake.count(), second_wake.count()), (1, 0));
    let successor = work.retained_producer().unwrap();
    assert!(unavailable(poll(&mut first, &first_wake)));
    assert_eq!(second_wake.count(), 1);
    assert!(unavailable(poll(&mut second, &second_wake)));
    let mut live = work
        .claim_when_released(1, 1, WorkCounters::UNCHARGED)
        .unwrap();
    assert!(matches!(poll(&mut live, &first_wake), Poll::Ready(Ok(_))));
    drop((first, second, live, blocker, successor));
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
}

#[test]
fn retained_original_cancellation_wakes_without_a_credit_release() {
    let (work, cancellation) = original();
    let producer = work.retained_producer().unwrap();
    let blocker = work.claim(49152, 1, WorkCounters::UNCHARGED).unwrap();
    let mut pending = work
        .claim_when_released(49152, 1, WorkCounters::UNCHARGED)
        .unwrap();
    let wake = Arc::new(Notifications::default());
    assert!(poll(&mut pending, &wake).is_pending());
    cancellation.cancel();
    assert!(wake.count() > 0);
    assert!(matches!(
        poll(&mut pending, &wake),
        Poll::Ready(Err(NativeTreeMaterializationError::Cancelled))
    ));
    drop((pending, blocker, producer));
    assert_eq!(
        work.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
}

#[test]
fn retained_impossible_claim_is_refused_before_waiting() {
    let (work, _) = original();
    let producer = work.retained_producer().unwrap();
    assert!(matches!(
        work.claim_when_released(65537, 1, WorkCounters::UNCHARGED),
        Err(NativeTreeMaterializationError::Work(
            crate::WorkError::BudgetExceeded { .. }
        ))
    ));
    drop(producer);
}
