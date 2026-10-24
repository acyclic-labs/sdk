//! Futures built on the heap, so callers hold a pointer instead of them.
//!
//! A future holds every future it is awaiting, so an operation whose layers
//! await each other is one value as large as its deepest path, and each poll
//! frame reserves room for the futures it builds and awaits. A native mount
//! callback polls a whole operation on the driver's thread, so that size
//! becomes stack depth: before this boundary existed, one lazy rename used
//! 1.8 MiB of a 2 MiB FUSE thread in an unoptimized build.
//!
//! [`in_heap`] builds a future in a short frame of its own and moves it into
//! a box. An async function whose body runs through it is a pointer to its
//! callers, and none of its state enters their futures or frames.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Builds `create`'s future directly in a heap allocation.
///
/// The future is built in this function's own frame, which returns before the
/// future is first polled, so no poll frame ever holds it.
///
/// This crate never calls it (`tests::every_frame_is_paid_for` scans the
/// source): a frame here is paid for on its work ledger first, through
/// [`in_heap_metered`], [`in_heap_budgeted`], [`in_heap_claimed`], [`in_heap_admitted`] or
/// [`in_heap_measured`], or is [`in_heap_unmetered`] with a stated reason.
#[inline(never)]
pub fn in_heap<F: Future>(create: impl FnOnce() -> F) -> Pin<Box<HeapFuture<F>>> {
    Box::pin(HeapFuture { future: create() })
}

/// [`in_heap`] for a call that holds no work ledger to pay for its frame.
///
/// Every call is preceded by an `// Unmetered:` comment naming why no ledger
/// exists there, or an `// Admitted above:` comment naming the ledger that
/// already holds the frame; `tests::every_frame_is_paid_for` enforces it.
#[inline(never)]
pub(crate) fn in_heap_unmetered<F: Future>(create: impl FnOnce() -> F) -> Pin<Box<HeapFuture<F>>> {
    Box::pin(HeapFuture { future: create() })
}

/// `Send` wherever provider futures are: an erased frame is a provider future.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) trait FrameSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> FrameSend for T {}
/// `Send` wherever provider futures are: an erased frame is a provider future.
#[cfg(target_arch = "wasm32")]
pub(crate) trait FrameSend {}
#[cfg(target_arch = "wasm32")]
impl<T> FrameSend for T {}

pub(crate) use acyclic_stream::BoxProviderFuture;

/// Re-types a heap frame as a provider future; nothing is reallocated.
///
/// A caller's auto-trait obligations stop at the erased frame instead of
/// descending through every nested frame, so boxing a deep chain stays within
/// the compiler's recursion limit wherever the chain is awaited.
pub(crate) fn erase<'a, F: Future + FrameSend + 'a>(
    frame: Pin<Box<HeapFuture<F>>>,
) -> BoxProviderFuture<'a, F::Output> {
    frame
}

pin_project_lite::pin_project! {
    /// A future whose poll boundary survives boxing and trait erasure.
    // Transparent layout preserves the admitted future's exact allocation size.
    #[repr(transparent)]
    pub struct HeapFuture<F> {
        #[pin]
        future: F,
    }
}

impl<F: Future> Future for HeapFuture<F> {
    type Output = F::Output;

    #[inline(never)]
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        self.project().future.poll(context)
    }
}

/// A ledger that pays for a heap frame before the frame is allocated.
///
/// Every metered path boxes its futures through [`in_heap_metered`] (or
/// [`metered!`]) or [`in_heap_claimed`], never through [`in_heap`]: the frame's
/// exact size is reserved first, so a refused or cancelled operation allocates
/// nothing, and the reservation is held for as long as the box exists.
pub(crate) trait FrameLedger {
    /// What the ledger holds while the frame is live.
    type Reservation;
    /// Why the ledger refused the frame.
    type Error;
    /// Reserves `bytes` for one allocation, or refuses without allocating.
    fn reserve_frame(&mut self, bytes: u64) -> Result<Self::Reservation, Self::Error>;
    /// Returns a frame's reservation once its box has been destroyed.
    fn release_frame(&mut self, reservation: Self::Reservation) -> Result<(), Self::Error>;
}

pub(crate) enum KernelFrameReservation {
    Standalone(crate::kernel::allocation::StandaloneFrameReservation),
    #[cfg(not(target_arch = "wasm32"))]
    Native(crate::NativeAllocation),
}

impl FrameLedger for crate::kernel::KernelAccounting<'_> {
    type Reservation = KernelFrameReservation;
    type Error = crate::FsError;

    fn reserve_frame(&mut self, bytes: u64) -> Result<Self::Reservation, crate::FsError> {
        self.check()?;
        match self {
            Self::Standalone {
                allocations,
                work,
                budget,
                ..
            } => allocations
                .claim_frame(bytes, work, *budget)
                .map(KernelFrameReservation::Standalone)
                .map_err(|error| {
                    error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                }),
            Self::BorrowedStandalone {
                allocations,
                work,
                budget,
                ..
            } => allocations
                .claim_frame(bytes, work, *budget)
                .map(KernelFrameReservation::Standalone)
                .map_err(|error| {
                    error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                }),
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeOriginal { work, .. } => work
                .claim(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED)
                .map(KernelFrameReservation::Native)
                .map_err(Into::into),
        }
    }

    fn release_frame(&mut self, reservation: Self::Reservation) -> Result<(), crate::FsError> {
        match reservation {
            KernelFrameReservation::Standalone(reservation) => drop(reservation),
            #[cfg(not(target_arch = "wasm32"))]
            KernelFrameReservation::Native(reservation) => drop(reservation),
        }
        Ok(())
    }
}
/// A pinned heap frame holding `F`.
pub(crate) type HeapFrame<F> = Pin<Box<HeapFuture<F>>>;

/// The exact allocation [`in_heap_metered`] and [`in_heap_claimed`] make for `F`.
fn frame_bytes<F>() -> u64 {
    crate::foundation::usize_to_u64(std::mem::size_of::<HeapFuture<F>>())
}

/// Builds `create`'s future in a heap frame `ledger` has already paid for.
///
/// The ledger is reborrowed for `create` only after it admitted the frame, so
/// a refusal returns before `create` runs or anything is allocated. Await the
/// result, then return its reservation with [`FrameLedger::release_frame`]:
/// [`metered!`] does both.
#[inline(never)]
pub(crate) fn in_heap_metered<'r, L: FrameLedger + ?Sized, F: Future>(
    ledger: &'r mut L,
    create: impl FnOnce(&'r mut L) -> F,
) -> Result<MeteredFrame<HeapFrame<F>, L::Reservation>, L::Error> {
    let reservation = ledger.reserve_frame(frame_bytes::<F>())?;
    Ok(MeteredFrame {
        frame: Some(Box::pin(HeapFuture {
            future: create(ledger),
        })),
        reservation: Some(reservation),
    })
}

/// Admits a backend frame before projecting the backend's remaining budget.
///
/// The frame remains live when its receipt joins the caller. Projecting first
/// would let the backend spend the bytes that are needed for its own frame.
#[inline(never)]
pub(crate) fn in_heap_budgeted<F: Future>(
    accounting: &mut crate::kernel::KernelAccounting<'_>,
    create: impl FnOnce(crate::WorkBudget) -> F,
) -> Result<MeteredFrame<HeapFrame<F>, KernelFrameReservation>, crate::FsError> {
    let reservation = accounting.reserve_frame(frame_bytes::<F>())?;
    let remaining = accounting.remaining()?;
    Ok(MeteredFrame {
        frame: Some(Box::pin(HeapFuture {
            future: create(remaining),
        })),
        reservation: Some(reservation),
    })
}

/// A paid heap frame. It destroys its box before handing back the reservation.
#[must_use = "a metered frame does nothing unless awaited"]
pub(crate) struct MeteredFrame<P, R> {
    frame: Option<P>,
    reservation: Option<R>,
}

impl<F: Future, R> MeteredFrame<Pin<Box<HeapFuture<F>>>, R> {
    /// Re-types the same paid box as a provider future; nothing is reallocated.
    pub(crate) fn erase<'a>(self) -> MeteredFrame<BoxProviderFuture<'a, F::Output>, R>
    where
        F: FrameSend + 'a,
    {
        MeteredFrame {
            frame: self.frame.map(erase),
            reservation: self.reservation,
        }
    }
}

impl<P: Future + Unpin, R: Unpin> Future for MeteredFrame<P, R> {
    type Output = (P::Output, R);

    #[inline(never)]
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(frame) = self.frame.as_mut() else {
            return Poll::Pending;
        };
        let output = std::task::ready!(Pin::new(frame).poll(context));
        self.frame = None;
        match self.reservation.take() {
            Some(reservation) => Poll::Ready((output, reservation)),
            None => Poll::Pending,
        }
    }
}

/// Builds `create`'s future in a heap frame claimed on the native `work`
/// ledger first. The claim is released when the frame is dropped, after the
/// box: on completion, on error and on cancellation alike.
#[cfg(not(target_arch = "wasm32"))]
#[inline(never)]
pub(crate) fn in_heap_claimed<F: Future>(
    work: &crate::NativeMaterializationWork,
    create: impl FnOnce() -> F,
) -> Result<ClaimedFrame<Pin<Box<HeapFuture<F>>>>, crate::NativeTreeMaterializationError> {
    let bytes = frame_bytes::<F>();
    let claim = work.claim(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED)?;
    Ok(ClaimedFrame {
        frame: Box::pin(HeapFuture { future: create() }),
        _claim: claim,
    })
}

/// A heap frame whose native claim outlives its box.
#[cfg(not(target_arch = "wasm32"))]
#[must_use = "a claimed frame does nothing unless awaited"]
pub(crate) struct ClaimedFrame<P> {
    // Declared first: the box is destroyed before the claim is released.
    frame: P,
    _claim: crate::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<P> ClaimedFrame<P> {
    /// Re-types the same box, for instance to erase it; nothing is reallocated.
    pub(crate) fn map_frame<Q>(self, map: impl FnOnce(P) -> Q) -> ClaimedFrame<Q> {
        ClaimedFrame {
            frame: map(self.frame),
            _claim: self._claim,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<P: Future + Unpin> Future for ClaimedFrame<P> {
    type Output = P::Output;

    #[inline(never)]
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<P::Output> {
        Pin::new(&mut self.frame).poll(context)
    }
}

/// Awaits `$call` in a frame `$ledger` paid for, then returns the reservation.
///
/// `$ledger` is a `&mut` place implementing [`FrameLedger`]; `$call` receives
/// it reborrowed as `$name`. Errors from either step convert with `?`.
macro_rules! metered {
    // Erased: the caller's auto-trait obligations stop at this frame. The
    // frame is erased in a statement of its own, so no temporary holding the
    // unerased type lives across the await.
    (erased $ledger:expr, |$name:ident| $call:expr) => {{
        let frame = crate::heap_future::in_heap_metered(&mut *$ledger, |$name| $call)?.erase();
        let (output, reservation) = frame.await;
        crate::heap_future::FrameLedger::release_frame(&mut *$ledger, reservation)?;
        output
    }};
    ($ledger:expr, |$name:ident| $call:expr) => {{
        let (output, reservation) =
            crate::heap_future::in_heap_metered(&mut *$ledger, |$name| $call)?.await;
        crate::heap_future::FrameLedger::release_frame(&mut *$ledger, reservation)?;
        output
    }};
}
pub(crate) use metered;

/// Admits a box into an existing ownership ledger before constructing its future.
/// The caller keeps that ledger in `state` and releases `bytes` after dropping
/// the completed box; this does not create or reset an operation receipt.
// Never inlined: the future's size is then computed for this function alone,
// not inside the optimization of every async caller that encloses it, where
// nested admitted futures would chain their layout queries without bound.
#[inline(never)]
pub(crate) fn in_heap_admitted<C, F: Future, E>(
    mut state: C,
    admit: impl FnOnce(&mut C, u64) -> Result<(), E>,
    create: impl FnOnce(C) -> F,
) -> Result<(HeapFrame<F>, u64), E> {
    let bytes = crate::foundation::usize_to_u64(std::mem::size_of::<F>());
    admit(&mut state, bytes)?;
    Ok((
        Box::pin(HeapFuture {
            future: create(state),
        }),
        bytes,
    ))
}

/// Known logical ownership of the existing boxed future, not proof DATA.
#[derive(Clone, Copy, Debug)]
pub(crate) enum HeapFutureEvent {
    BeforeFrame {
        bytes: u64,
        budget: crate::WorkBudget,
    },
    Admitted(crate::WorkCounters),
    Released(u64),
}

type HeapFutureObserver<'a> = &'a (dyn Fn(HeapFutureEvent) -> Result<u64, crate::WorkError> + Sync);

struct HeapFutureRelease<'a> {
    observer: Option<HeapFutureObserver<'a>>,
    bytes: u64,
}

impl Drop for HeapFutureRelease<'_> {
    fn drop(&mut self) {
        if let Some(observer) = self.observer {
            observer(HeapFutureEvent::Released(self.bytes))
                .expect("the actual admitted SDK future must release once");
        }
    }
}

/// Preserves the short-frame boundary while admitting its actual future body.
/// The nested permit reserves the live box until the operation has completed.
pub(crate) async fn in_heap_measured<F, T, E>(
    create: impl FnOnce(crate::WorkBudget) -> F,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
    cancelled: E,
    observer: Option<HeapFutureObserver<'_>>,
) -> crate::MeasuredResult<crate::OperationReceipt<T>, E>
where
    F: Future<Output = crate::MeasuredResult<crate::OperationReceipt<T>, E>>,
    E: From<crate::WorkError>,
{
    in_heap_measured_with_error(create, budget, cancellation, cancelled, observer, E::from).await
}

/// Maps internal admission errors through the same SDK error as the operation.
pub(crate) async fn in_heap_measured_with_error<F, T, E>(
    create: impl FnOnce(crate::WorkBudget) -> F,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
    cancelled: E,
    observer: Option<HeapFutureObserver<'_>>,
    map_work: impl Fn(crate::WorkError) -> E,
) -> crate::MeasuredResult<crate::OperationReceipt<T>, E>
where
    F: Future<Output = crate::MeasuredResult<crate::OperationReceipt<T>, E>>,
{
    let mut work = crate::WorkCounters::default();
    if cancellation.check().is_err() {
        return Err(crate::OperationFailure::new(cancelled, work));
    }
    let bytes = crate::foundation::usize_to_u64(std::mem::size_of::<F>());
    let standing = match observer {
        Some(observer) => observer(HeapFutureEvent::BeforeFrame { bytes, budget })
            .map_err(|error| crate::OperationFailure::before_work(map_work(error)))?,
        None => 0,
    };
    let mut frame_budget = budget;
    frame_budget.peak_allocation_bytes = frame_budget
        .peak_allocation_bytes
        .checked_sub(standing)
        .ok_or_else(|| {
        crate::OperationFailure::before_work(map_work(crate::WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed: standing,
            maximum: budget.peak_allocation_bytes,
        }))
    })?;
    work.charge_allocation(u64::from(bytes != 0), bytes, &frame_budget)
        .map_err(|error| crate::OperationFailure::new(map_work(error), work))?;
    if let Some(observer) = observer {
        observer(HeapFutureEvent::Admitted(work))
            .map_err(|error| crate::OperationFailure::before_work(map_work(error)))?;
    }
    // Declared before all fallible bookkeeping and the box so admission retires
    // on errors and unwinding, after the whole future has been destroyed.
    let frame = HeapFutureRelease { observer, bytes };
    work.peak_allocation_bytes = work
        .peak_allocation_bytes
        .checked_add(standing)
        .ok_or_else(|| crate::OperationFailure::new(map_work(crate::WorkError::Overflow), work))?;
    let mut remaining = work
        .remaining(budget)
        .map_err(|error| crate::OperationFailure::new(map_work(error), work))?;
    // The existing cache leaf reserves its own backing. Only this new frame
    // is delegated here, otherwise the very same cache would be charged twice.
    remaining.peak_allocation_bytes = remaining
        .peak_allocation_bytes
        .checked_sub(bytes)
        .ok_or_else(|| crate::OperationFailure::new(map_work(crate::WorkError::Overflow), work))?;
    // Admitted above; built in its own short frame, not this poll frame.
    let mut future = in_heap_unmetered(move || create(remaining));
    let result = future.as_mut().await;
    drop(future);
    drop(frame);
    match result {
        Err(mut failure) => {
            // Ordinary failures already own their receipt. Retain that same
            // allocation and the whole caller error while joining frame work.
            *failure.work = work.with_backend(*failure.work, bytes).unwrap_or(work);
            Err(failure)
        }
        Ok(receipt) => {
            work = work
                .with_backend(receipt.work, bytes)
                .map_err(|error| crate::OperationFailure::new(map_work(error), work))?;
            work.verify(budget)
                .map_err(|error| crate::OperationFailure::new(map_work(error), work))?;
            Ok(crate::OperationReceipt {
                value: receipt.value,
                work,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    /// Every heap frame this crate builds is paid for, or says why not.
    #[test]
    fn every_frame_is_paid_for() {
        fn scan(directory: &std::path::Path, violations: &mut Vec<String>) {
            for entry in std::fs::read_dir(directory).expect("source directory") {
                let path = entry.expect("source entry").path();
                if path.is_dir() {
                    scan(&path, violations);
                    continue;
                }
                if path.extension().is_none_or(|extension| extension != "rs")
                    || path.ends_with("heap_future.rs")
                {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("source file");
                let lines: Vec<&str> = source.lines().collect();
                for (index, line) in lines.iter().enumerate() {
                    let code = line.split("//").next().unwrap_or_default();
                    let raw = code.match_indices("in_heap(").any(|(at, _)| {
                        !code
                            .get(..at)
                            .unwrap_or_default()
                            .ends_with(|c: char| c == '_' || c.is_alphanumeric())
                    });
                    let justified = lines[index.saturating_sub(4)..index].iter().any(|above| {
                        let above = above.trim_start();
                        above.starts_with("// Unmetered:")
                            || above.starts_with("// Admitted above:")
                    });
                    if raw || (code.contains("in_heap_unmetered(") && !justified) {
                        violations.push(format!("{}:{}", path.display(), index + 1));
                    }
                }
            }
        }
        let mut violations = Vec::new();
        scan(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut violations,
        );
        assert!(violations.is_empty(), "unpaid heap frames: {violations:?}");
    }

    struct Refusing;

    impl super::FrameLedger for Refusing {
        type Reservation = ();
        type Error = ();
        fn reserve_frame(&mut self, _bytes: u64) -> Result<(), ()> {
            Err(())
        }
        fn release_frame(&mut self, (): ()) -> Result<(), ()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn backend_budget_excludes_its_live_frame_before_construction() {
        let cancellation = crate::CancellationToken::new();
        let mut budget = crate::WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = 64_000;
        let mut accounting = crate::kernel::KernelAccounting::standalone(budget, &cancellation);
        let (delegated, reservation) = super::in_heap_budgeted(&mut accounting, std::future::ready)
            .expect("backend frame admitted")
            .await;
        let (allocations, spent, _, _) = accounting.standalone_parts().unwrap();
        let live = allocations.live_bytes();
        assert!(live > 0);
        assert_eq!(
            delegated.peak_allocation_bytes + live,
            budget.peak_allocation_bytes
        );
        let backend = crate::WorkCounters {
            peak_allocation_bytes: delegated.peak_allocation_bytes,
            ..crate::WorkCounters::UNCHARGED
        };
        spent
            .with_backend(backend, live)
            .unwrap()
            .verify(budget)
            .unwrap();
        super::FrameLedger::release_frame(&mut accounting, reservation).unwrap();
    }

    /// A refused frame builds nothing: its future is never created.
    #[test]
    fn a_refused_frame_is_never_built() {
        let mut built = false;
        let refused = super::in_heap_metered(&mut Refusing, |_| {
            built = true;
            std::future::ready(())
        });
        assert!(refused.is_err());
        assert!(!built, "the future was built before the ledger admitted it");
    }

    /// A cancelled kernel ledger refuses the frame before it is built.
    #[test]
    fn a_cancelled_operation_builds_no_frame() {
        let cancellation = crate::CancellationToken::new();
        cancellation.cancel();
        let mut accounting = crate::kernel::KernelAccounting::standalone(
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut built = false;
        let refused = super::in_heap_metered(&mut accounting, |_| {
            built = true;
            std::future::ready(())
        });
        assert!(refused.is_err());
        assert!(!built);
    }

    #[tokio::test]
    async fn standalone_frames_retire_when_a_borrowing_operation_is_dropped() {
        async fn exercise(accounting: &mut crate::kernel::KernelAccounting<'_>) {
            let ((), reservation) = super::in_heap_metered(accounting, |_| std::future::ready(()))
                .expect("first frame and retirement cell admitted")
                .await;
            super::FrameLedger::release_frame(accounting, reservation).expect("frame retired");
            let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
            let mut frame = super::in_heap_metered(accounting, |_| async {
                let retained = [0_u8; 128];
                std::future::pending::<()>().await;
                std::hint::black_box(retained);
            })
            .expect("pending frame admitted");
            assert!(
                std::future::Future::poll(
                    std::pin::Pin::new(&mut frame),
                    &mut std::task::Context::from_waker(std::task::Waker::noop()),
                )
                .is_pending()
            );
            drop(frame);
            assert_eq!(
                accounting.standalone_parts().unwrap().0.live_bytes(),
                baseline,
                "dropping the operation must retire its standalone frame"
            );
            let ((), reservation) = super::in_heap_metered(accounting, |_| std::future::ready(()))
                .expect("ledger is reusable after cancellation")
                .await;
            super::FrameLedger::release_frame(accounting, reservation).expect("frame retired");
            assert_eq!(
                accounting.standalone_parts().unwrap().0.live_bytes(),
                baseline
            );
        }
        let cancellation = crate::CancellationToken::new();
        exercise(&mut crate::kernel::KernelAccounting::standalone(
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await;
        let mut ledger = crate::kernel::allocation::AllocationLedger::default();
        let mut work = crate::WorkCounters::UNCHARGED;
        exercise(&mut crate::kernel::KernelAccounting::borrowed_standalone(
            &mut ledger,
            &mut work,
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await;
    }

    #[test]
    fn refusal_admits_neither_the_frame_nor_its_retirement_cell() {
        let cancellation = crate::CancellationToken::new();
        let mut accounting = crate::kernel::KernelAccounting::standalone(
            crate::WorkBudget {
                allocation_operations: 0,
                ..crate::WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        let mut built = false;
        assert!(
            super::in_heap_metered(&mut accounting, |_| {
                built = true;
                std::future::ready(())
            })
            .is_err()
        );
        assert!(!built);
        assert_eq!(accounting.spent(), crate::WorkCounters::UNCHARGED);
        assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);
    }

    #[test]
    fn cumulative_frame_retirement_cannot_wrap() {
        let mut ledger = crate::kernel::allocation::AllocationLedger::default();
        let mut work = crate::WorkCounters::UNCHARGED;
        drop(
            ledger
                .claim_frame(0, &mut work, crate::WorkBudget::UNBOUNDED)
                .expect("retirement control admitted"),
        );
        let baseline = ledger.live_bytes();
        let frame = ledger
            .claim_frame(u64::MAX - baseline, &mut work, crate::WorkBudget::UNBOUNDED)
            .expect("largest logical frame admitted");
        drop(frame);
        assert_eq!(ledger.live_bytes(), baseline);
        let before = work;
        assert!(
            ledger
                .claim_frame(baseline + 1, &mut work, crate::WorkBudget::UNBOUNDED)
                .is_err()
        );
        assert_eq!(work, before, "refused frame changed completed work");
        assert_eq!(ledger.live_bytes(), baseline);
    }
    #[test]
    fn previews_preserve_retirement_on_commit_and_discard() {
        for commit in [false, true] {
            let mut ledger = crate::kernel::allocation::AllocationLedger::default();
            let mut work = crate::WorkCounters::UNCHARGED;
            let frame = ledger
                .claim_frame(64, &mut work, crate::WorkBudget::UNBOUNDED)
                .expect("frame admitted");
            let baseline = ledger.live_bytes() - 64;
            let mut preview = ledger.preview().expect("frame retirements balance");
            // Retire an existing frame while the preview holds the ledger;
            // the preview must preserve that retirement on either outcome.
            drop(frame);
            preview
                .claim_bytes(0, 0, &mut work, crate::WorkBudget::UNBOUNDED)
                .expect("numeric preview admitted");
            if commit {
                preview.commit().expect("frame retirements balance");
            } else {
                drop(preview);
            }
            assert_eq!(ledger.live_bytes(), baseline);
            let later = ledger
                .claim_frame(128, &mut work, crate::WorkBudget::UNBOUNDED)
                .expect("later admission is isolated from the completed preview");
            assert_eq!(ledger.live_bytes(), baseline + 128);
            drop(later);
            assert_eq!(ledger.live_bytes(), baseline);
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn a_refused_native_frame_is_never_built() {
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                allocation_operations: 1,
                ..crate::WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .expect("ledger itself is admitted");
        let mut built = false;
        let frame = super::in_heap_claimed(&work, || {
            built = true;
            std::future::ready(())
        });
        assert!(frame.is_err());
        assert!(
            !built,
            "native frame was built before its claim was admitted"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn a_cancelled_native_operation_builds_no_frame() {
        let cancellation = crate::CancellationToken::new();
        let work =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .expect("ledger is admitted before cancellation");
        cancellation.cancel();
        let mut built = false;
        let frame = super::in_heap_claimed(&work, || {
            built = true;
            std::future::ready(())
        });
        assert!(frame.is_err());
        assert!(!built);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_claim_outlives_the_frame_on_completion_and_cancellation() {
        struct CheckedDrop<'a> {
            work: &'a crate::NativeMaterializationWork,
            baseline: u64,
        }
        impl std::future::Future for CheckedDrop<'_> {
            type Output = ();
            fn poll(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<()> {
                std::task::Poll::Ready(())
            }
        }
        impl Drop for CheckedDrop<'_> {
            fn drop(&mut self) {
                assert_eq!(
                    self.work.state(|state| state.live_allocation_bytes()),
                    self.baseline + super::frame_bytes::<Self>(),
                    "claim was released before destroying the frame"
                );
            }
        }
        let cancellation = crate::CancellationToken::new();
        let work =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .expect("ledger is admitted");
        let baseline = work.state(|state| state.live_allocation_bytes());
        let make = || {
            super::in_heap_claimed(&work, || CheckedDrop {
                work: &work,
                baseline,
            })
        };
        make().expect("completed frame admitted").await;
        assert_eq!(work.state(|state| state.live_allocation_bytes()), baseline);
        let cancelled_frame = make().expect("cancelled frame admitted");
        drop(cancelled_frame);
        assert_eq!(work.state(|state| state.live_allocation_bytes()), baseline);
    }
    use super::{HeapFutureEvent, in_heap_admitted, in_heap_measured};
    use crate::kernel::allocation::AllocationLedger;
    use crate::{
        CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget,
        WorkCounters, WorkError,
    };
    use std::cell::Cell;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[derive(Debug, Eq, PartialEq)]
    enum TestError {
        Cancelled,
        Work(WorkError),
    }

    impl From<WorkError> for TestError {
        fn from(error: WorkError) -> Self {
            Self::Work(error)
        }
    }

    struct CopyFuture {
        budget: WorkBudget,
        copied: [u8; 8],
    }

    impl Future for CopyFuture {
        type Output = MeasuredResult<OperationReceipt<[u8; 8]>, TestError>;

        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            let mut work = WorkCounters::default();
            let result = (|| {
                work.charge_items(2, &self.budget)?;
                let delta = WorkCounters {
                    bytes_copied: 4,
                    ..WorkCounters::default()
                };
                work.charge(&delta, &self.budget)?;
                self.copied[..4].copy_from_slice(b"heap");
                work.charge(&delta, &self.budget)?;
                self.copied[4..].copy_from_slice(b"body");
                Ok(self.copied)
            })();
            Poll::Ready(
                result
                    .map(|value| OperationReceipt { value, work })
                    .map_err(|error: WorkError| OperationFailure::new(error.into(), work)),
            )
        }
    }

    fn copy_budget() -> WorkBudget {
        WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: crate::foundation::usize_to_u64(
                std::mem::size_of::<CopyFuture>(),
            ),
            items_examined: 2,
            bytes_copied: 8,
            ..WorkBudget::default()
        }
    }

    #[tokio::test]
    async fn cold_cancellation_and_future_denial_do_not_construct_body() {
        let created = Cell::new(0);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let failure = in_heap_measured(
            |budget| {
                created.set(created.get() + 1);
                CopyFuture {
                    budget,
                    copied: [0; 8],
                }
            },
            copy_budget(),
            &cancelled,
            TestError::Cancelled,
            None,
        )
        .await
        .expect_err("cold cancellation");
        assert_eq!(failure.error, TestError::Cancelled);
        assert_eq!(*failure.work, WorkCounters::default());

        let cancellation = CancellationToken::new();
        for budget in [
            WorkBudget {
                allocation_operations: 0,
                ..copy_budget()
            },
            WorkBudget {
                peak_allocation_bytes: copy_budget().peak_allocation_bytes - 1,
                ..copy_budget()
            },
        ] {
            let failure = in_heap_measured(
                |budget| {
                    created.set(created.get() + 1);
                    CopyFuture {
                        budget,
                        copied: [0; 8],
                    }
                },
                budget,
                &cancellation,
                TestError::Cancelled,
                None,
            )
            .await
            .expect_err("future admission");
            assert!(matches!(
                failure.error,
                TestError::Work(WorkError::BudgetExceeded { .. })
            ));
            assert_eq!(*failure.work, WorkCounters::default());
        }
        assert_eq!(created.get(), 0);
    }

    #[tokio::test]
    async fn exact_future_permit_counts_envelope_once_and_preserves_copy_failure() {
        let cancellation = CancellationToken::new();
        let receipt = in_heap_measured(
            |budget| {
                assert_eq!(budget.allocation_operations, 0);
                assert_eq!(budget.peak_allocation_bytes, 0);
                CopyFuture {
                    budget,
                    copied: [0; 8],
                }
            },
            copy_budget(),
            &cancellation,
            TestError::Cancelled,
            None,
        )
        .await
        .expect("exact permit");
        assert_eq!(receipt.value, *b"heapbody");
        assert_eq!(receipt.work, copy_budget());

        let budget = WorkBudget {
            bytes_copied: 7,
            ..copy_budget()
        };
        let failure = in_heap_measured(
            |budget| CopyFuture {
                budget,
                copied: [0; 8],
            },
            budget,
            &cancellation,
            TestError::Cancelled,
            None,
        )
        .await
        .expect_err("second copy denied");
        assert_eq!(
            failure.error,
            TestError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied",
                observed: 8,
                maximum: 7,
            })
        );
        assert_eq!(
            *failure.work,
            WorkCounters {
                bytes_copied: 4,
                ..copy_budget()
            }
        );
    }

    #[tokio::test]
    async fn cache_standing_and_actual_frame_share_exact_peak_without_double_delegation() {
        let cancellation = CancellationToken::new();
        let cache = Box::new([0_u8; 13]);
        let admitted = std::sync::atomic::AtomicU64::new(0);
        let released = std::sync::atomic::AtomicU64::new(0);
        let trace = |event| {
            match event {
                HeapFutureEvent::BeforeFrame { .. } => return Ok(cache.len() as u64),
                HeapFutureEvent::Admitted(work) => {
                    admitted.fetch_add(
                        work.allocation_operations,
                        std::sync::atomic::Ordering::AcqRel,
                    );
                }
                HeapFutureEvent::Released(bytes) => {
                    released.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
                }
            }
            Ok(0)
        };
        let budget = WorkBudget {
            peak_allocation_bytes: copy_budget().peak_allocation_bytes + cache.len() as u64,
            ..copy_budget()
        };
        let receipt = in_heap_measured(
            |remaining| {
                assert_eq!(remaining.peak_allocation_bytes, cache.len() as u64);
                CopyFuture {
                    budget: remaining,
                    copied: [0; 8],
                }
            },
            budget,
            &cancellation,
            TestError::Cancelled,
            Some(&trace),
        )
        .await
        .expect("exact standing plus frame");
        assert_eq!(receipt.value, *b"heapbody");
        assert_eq!(receipt.work, budget);
        assert_eq!(admitted.load(std::sync::atomic::Ordering::Acquire), 1);
        assert_eq!(
            released.load(std::sync::atomic::Ordering::Acquire),
            copy_budget().peak_allocation_bytes
        );

        let created = Cell::new(false);
        let failure = in_heap_measured(
            |remaining| {
                created.set(true);
                CopyFuture {
                    budget: remaining,
                    copied: [0; 8],
                }
            },
            WorkBudget {
                peak_allocation_bytes: budget.peak_allocation_bytes - 1,
                ..budget
            },
            &cancellation,
            TestError::Cancelled,
            Some(&trace),
        )
        .await
        .expect_err("cache and frame overlap denied");
        assert!(!created.get());
        assert_eq!(*failure.work, WorkCounters::UNCHARGED);
        assert_eq!(admitted.load(std::sync::atomic::Ordering::Acquire), 1);
        assert_eq!(
            released.load(std::sync::atomic::Ordering::Acquire),
            copy_budget().peak_allocation_bytes
        );
    }

    #[tokio::test]
    async fn shared_future_uses_existing_ledger_without_resetting_work_or_live_bytes() {
        let mut work = WorkCounters::default();
        let mut allocations = AllocationLedger::default();
        allocations
            .claim_bytes(17, 1, &mut work, WorkBudget::UNBOUNDED)
            .expect("parent allocation");
        let parent = Box::new([0_u8; 17]);
        work.charge_items(3, &WorkBudget::UNBOUNDED)
            .expect("parent prefix");
        let created = Cell::new(false);
        let admitted_bytes = Cell::new(0);
        let (mut future, bytes) = in_heap_admitted(
            (&mut work, &mut allocations),
            |(work, allocations), bytes| {
                admitted_bytes.set(bytes);
                allocations.claim_bytes(bytes, u64::from(bytes != 0), work, WorkBudget::UNBOUNDED)
            },
            |(work, allocations)| {
                created.set(true);
                let admitted_bytes = &admitted_bytes;
                async move {
                    assert_eq!(allocations.live_bytes(), 17 + admitted_bytes.get());
                    assert_eq!(work.items_examined, 3);
                    work.charge_items(1, &WorkBudget::UNBOUNDED)
                }
            },
        )
        .expect("shared admission");
        assert!(created.get());
        assert_eq!(
            bytes,
            crate::foundation::usize_to_u64(std::mem::size_of_val(future.as_ref().get_ref()))
        );
        future.as_mut().await.expect("shared body");
        drop(future);
        assert_eq!(allocations.live_bytes(), 17 + bytes);
        allocations.release(bytes).expect("release completed box");
        assert_eq!(allocations.live_bytes(), 17);
        assert_eq!(work.items_examined, 4);
        assert_eq!(work.allocation_operations, 2);
        assert_eq!(work.peak_allocation_bytes, 17 + bytes);
        assert_eq!(parent.len(), 17);
        drop(parent);
        allocations.release(17).expect("release parent");
    }

    #[test]
    fn shared_future_denial_keeps_the_original_prefix_and_never_constructs_body() {
        let mut work = WorkCounters::default();
        let mut allocations = AllocationLedger::default();
        allocations
            .claim_bytes(17, 1, &mut work, WorkBudget::UNBOUNDED)
            .expect("parent allocation");
        let parent = Box::new([0_u8; 17]);
        work.charge_items(3, &WorkBudget::UNBOUNDED)
            .expect("parent prefix");
        let prior = work;
        let created = Cell::new(false);
        let result = in_heap_admitted(
            (&mut work, &mut allocations),
            |(work, allocations), bytes| {
                let budget = WorkBudget {
                    allocation_operations: 1,
                    peak_allocation_bytes: 17 + bytes,
                    items_examined: 3,
                    ..WorkBudget::default()
                };
                allocations.claim_bytes(bytes, u64::from(bytes != 0), work, budget)
            },
            |_| {
                created.set(true);
                std::future::ready(())
            },
        );
        assert!(result.is_err());
        assert!(!created.get());
        assert_eq!(work, prior);
        assert_eq!(allocations.live_bytes(), 17);
        assert_eq!(parent.len(), 17);
        drop(parent);
        allocations.release(17).expect("release parent");
    }

    #[tokio::test]
    async fn observer_future_panic_preserves_exact_frame_and_data_prefix_after_box_drop() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct PanicDataFuture<'a> {
            owner: &'a crate::kernel::ProofDataCustody,
            dropped: &'a AtomicBool,
            budget: WorkBudget,
        }
        impl Future for PanicDataFuture<'_> {
            type Output = MeasuredResult<OperationReceipt<()>, TestError>;

            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
                let mut work = WorkCounters::default();
                let mut allocations = AllocationLedger::default();
                allocations
                    .claim_bytes(3, 1, &mut work, self.budget)
                    .expect("actual DATA admission");
                self.owner
                    .record_data_work(work)
                    .expect("admitted DATA prefix");
                let mut data = Vec::new();
                data.try_reserve_exact(3).expect("three actual DATA bytes");
                let copy = WorkCounters {
                    bytes_copied: 3,
                    ..WorkCounters::default()
                };
                work.charge(&copy, &self.budget)
                    .expect("actual copy admission");
                self.owner
                    .record_data_work(WorkCounters {
                        peak_allocation_bytes: 3,
                        ..copy
                    })
                    .expect("completed copy prefix");
                data.extend_from_slice(b"cow");
                panic!("backend unwind after actual SDK DATA copy");
            }
        }
        impl Drop for PanicDataFuture<'_> {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::Release);
            }
        }

        let owner = crate::kernel::ProofDataCustody::default();
        let dropped = AtomicBool::new(false);
        let observer = |event| {
            if matches!(event, super::HeapFutureEvent::Released(_)) {
                assert!(
                    dropped.load(Ordering::Acquire),
                    "the entire box must die before release"
                );
            }
            owner.record_heap_future_event(event)
        };
        let panic = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(async {
            (in_heap_measured(
                |budget| PanicDataFuture {
                    owner: &owner,
                    dropped: &dropped,
                    budget,
                },
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                TestError::Cancelled,
                Some(&observer),
            ))
            .await
        }))
        .await;
        assert!(panic.is_err());
        assert_eq!(
            owner.retained_bytes(),
            0,
            "frames are not proof DATA adoption"
        );
        let prefix = owner.take_completed_data_work();
        assert_eq!(prefix.allocation_operations, 2);
        assert_eq!(prefix.bytes_copied, 3);
        assert_eq!(
            prefix.peak_allocation_bytes,
            crate::foundation::usize_to_u64(std::mem::size_of::<PanicDataFuture<'_>>()) + 3
        );

        // A later segment must not inherit the retired frame's live bytes.
        owner
            .record_data_work(WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: 1,
                ..WorkCounters::default()
            })
            .expect("later actual DATA");
        assert_eq!(owner.take_completed_data_work().peak_allocation_bytes, 1);
    }
}
