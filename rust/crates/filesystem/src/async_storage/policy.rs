use super::*;
use std::cell::Cell;

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
            let future = futures::future::poll_fn(|_| {
                assert_eq!(PolicyCut::current().map(|current| current.authority), Some(cut.authority));
                if complete { std::task::Poll::Ready(()) } else { std::task::Poll::Pending }
            });
            let mut future = Box::pin(cut.scope(future));
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
#[derive(Clone, Copy, Debug)]
pub struct PolicyCut {
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) owner: [u8; 16],
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) authority: AuthorityId,
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) head: Head,
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) epoch_tail: u64,
}

thread_local! {
    // Installed only while polling the owning future, never across a yield.
    // No durable lock or background cleanup can survive cancellation or exit.
    static CURRENT: Cell<Option<PolicyCut>> = const { Cell::new(None) };
}

impl PolicyCut {
    #[cfg(any(test, feature = "distributed"))]
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

    pub(crate) fn current() -> Option<Self> {
        CURRENT.get()
    }

    pub(crate) async fn scope<F: Future>(self, future: F) -> F::Output {
        struct Restore(Option<PolicyCut>);
        impl Drop for Restore {
            fn drop(&mut self) {
                CURRENT.set(self.0);
            }
        }
        let mut future = std::pin::pin!(future);
        futures::future::poll_fn(|cx| {
            let _restore = Restore(CURRENT.replace(Some(self)));
            future.as_mut().poll(cx)
        })
        .await
    }
}
