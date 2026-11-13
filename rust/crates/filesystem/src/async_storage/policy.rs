use super::*;
use crate::kernel::allocation::retained::RetainedAllocation;
use std::cell::RefCell;

/// The backend's authorization/publication coordination guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyOwner {
    /// No policy coordination guarantee is declared.
    Unsupported,
    /// All mutations share one exclusive process owner and its policy registry.
    ExclusiveProcess,
    /// Publications atomically check the captured durable policy source head.
    /// This does not authorize physical projections or idle process-local watches.
    AtomicPublication,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn policy_cut_scope_never_leaks_across_pending_ready_or_drop(polls in 0usize..32, complete in any::<bool>()) {
            let cut = PolicyCut::new([1; 16], AuthorityId::from_bytes([2; 16]), Head::genesis(Epoch::GENESIS), 1);
            let future = futures::future::poll_fn(move |_| {
                assert!(PolicyCut::current().is_some_and(|captured| captured.as_slice() == [cut]));
                if complete { std::task::Poll::Ready(()) } else { std::task::Poll::Pending }
            });
            let body_bytes = std::mem::size_of_val(&future);
            let scoped = cut.scope(future);
            // One body and one cut, allowing alignment at both field boundaries.
            let maximum = body_bytes + std::mem::size_of::<Option<ScopedCuts>>() + 2 * std::mem::size_of::<usize>();
            prop_assert!(std::mem::size_of_val(&scoped) <= maximum, "policy scope duplicates its body: {} bytes, single-body maximum {}", std::mem::size_of_val(&scoped), maximum);
            let mut future = Box::pin(scoped);
            let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
            for _ in 0..polls {
                let _ = future.as_mut().poll(&mut cx);
                prop_assert!(PolicyCut::current().is_none());
                if complete { break; }
            }
            drop(future);
            prop_assert!(PolicyCut::current().is_none());
        }
    }
}

/// An authenticated backend cut. Only the backend can construct its evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolicyCut {
    pub(crate) owner: [u8; 16],
    pub(crate) authority: AuthorityId,
    pub(crate) head: Head,
    pub(crate) epoch_tail: u64,
}

thread_local! {
    // Installed only while polling the owning future, never across a yield.
    // No durable lock or background cleanup can survive cancellation or exit.
    static CURRENT: RefCell<Option<ScopedCuts>> = const { RefCell::new(None) };
}

impl PolicyCut {
    pub(crate) fn new(
        owner: [u8; 16],
        authority: AuthorityId,
        head: Head,
        epoch_tail: u64,
    ) -> Self {
        Self {
            owner,
            authority,
            head,
            epoch_tail,
        }
    }

    pub(crate) fn current() -> Option<ScopedCuts> {
        CURRENT.with(|current| current.borrow().clone())
    }

    /// Apply this backend-authenticated cut only while polling `future`.
    /// Publication must be awaited inside the future, never spawned separately.
    /// The original cut is restored on Pending, completion and cancellation.
    pub fn scope<F: Future>(self, future: F) -> impl Future<Output = F::Output> {
        PolicyFuture {
            cuts: Some(ScopedCuts(ScopedCutStorage::One(self))),
            future,
        }
    }
}

pin_project_lite::pin_project! {
    struct PolicyFuture<F> {
        cuts: Option<ScopedCuts>,
        #[pin]
        future: F,
    }
}

impl<F: Future> Future for PolicyFuture<F> {
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        struct Restore(Option<ScopedCuts>);
        impl Drop for Restore {
            fn drop(&mut self) {
                let retired = CURRENT.with(|current| current.replace(self.0.take()));
                drop(retired);
            }
        }
        let this = self.project();
        let _restore = this
            .cuts
            .as_ref()
            .map(|cuts| Restore(CURRENT.with(|current| current.replace(Some(cuts.clone())))));
        this.future.poll(cx)
    }
}

/// Several authenticated source cuts retained on their original work owner.
/// A scope contributes every cut to the same existing publication mechanism.
/// An empty group preserves the enclosing scope; a single cut allocates nothing.
#[derive(Clone)]
pub struct PolicyGroup {
    cuts: Option<ScopedCuts>,
}

#[derive(Clone)]
pub(crate) struct ScopedCuts(ScopedCutStorage);
#[derive(Clone)]
enum ScopedCutStorage {
    One(PolicyCut),
    Many(Arc<RetainedCuts>),
}
struct RetainedCuts {
    cuts: Vec<PolicyCut>,
    // The original backing dies after its vector and last scope/proposal owner.
    _allocation: RetainedAllocation,
}
impl ScopedCuts {
    pub(crate) fn as_slice(&self) -> &[PolicyCut] {
        match &self.0 {
            ScopedCutStorage::One(cut) => std::slice::from_ref(cut),
            ScopedCutStorage::Many(cuts) => &cuts.cuts,
        }
    }
}
impl PolicyGroup {
    /// Funds one authenticated cut copy before allocation on the supplied work.
    /// Retaining or polling this group shares its backing without allocating.
    pub fn new<W: crate::facade::FsWork>(
        cuts: &[PolicyCut],
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, crate::FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| -> Result<Self, crate::FsError> {
            accounting.check()?;
            let bytes = cuts
                .len()
                .checked_mul(std::mem::size_of::<PolicyCut>())
                .ok_or(crate::WorkError::Overflow)? as u64;
            let copying = WorkCounters {
                items_examined: cuts.len() as u64,
                bytes_copied: bytes,
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(copying)?;
            let cuts = match cuts {
                [] => None,
                [cut] => {
                    accounting.charge(copying)?;
                    Some(ScopedCuts(ScopedCutStorage::One(*cut)))
                }
                _ => {
                    let shared = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<
                        RetainedCuts,
                    >())
                    .map_err(|_| crate::WorkError::Overflow)?
                        as u64;
                    let allocation = RetainedAllocation::claim(
                        shared
                            .checked_add(bytes)
                            .ok_or(crate::WorkError::Overflow)?,
                        2,
                        &mut accounting,
                    )?;
                    accounting.charge(copying)?;
                    Some(ScopedCuts(ScopedCutStorage::Many(Arc::new(RetainedCuts {
                        cuts: cuts.to_vec(),
                        _allocation: allocation,
                    }))))
                }
            };
            Ok(Self { cuts })
        })();
        work.finish(result, &accounting)
    }

    /// Await publication inside this scope; all source heads and retirements
    /// are conditioned in its one atomic commit, including combined proposals.
    pub fn scope<F: Future>(self, future: F) -> impl Future<Output = F::Output> {
        PolicyFuture {
            cuts: self.cuts,
            future,
        }
    }
}

#[cfg(test)]
mod grouped_tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn grouped_scopes_restore_every_cut_after_pending_ready_and_drop(polls in 0usize..16, complete in any::<bool>(), empty in any::<bool>()) {
            let token = CancellationToken::new();
            let first = PolicyCut::new([3; 16], AuthorityId::from_bytes([4; 16]), Head::genesis(Epoch::GENESIS), 1);
            let second = PolicyCut::new([3; 16], AuthorityId::from_bytes([5; 16]), Head::genesis(Epoch::GENESIS), 1);
            let expected = if empty { vec![first] } else { vec![first, second] };
            let group = PolicyGroup::new(if empty { &[] } else { &[first, second] }, WorkBudget::UNBOUNDED, &token).unwrap().value;
            let mut future = Box::pin(first.scope(group.scope(futures::future::poll_fn(move |_| {
                assert!(PolicyCut::current().is_some_and(|current| current.as_slice() == expected));
                if complete { std::task::Poll::Ready(()) } else { std::task::Poll::Pending }
            }))));
            let mut context = std::task::Context::from_waker(futures::task::noop_waker_ref());
            for _ in 0..polls {
                let _ = future.as_mut().poll(&mut context);
                prop_assert!(PolicyCut::current().is_none());
                if complete { break; }
            }
            drop(future);
            prop_assert!(PolicyCut::current().is_none());
        }
    }
}
