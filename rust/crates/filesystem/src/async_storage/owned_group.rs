//! Owned handoff to the same authority append builder, without a caller borrow.
use super::*;
use crate::kernel::allocation::retained::{OwnedAccounting, Retained, RetainedAllocation};

/// An authority seal whose ownership survives a client dropping its reply.
pub trait OwnedAuthorityGuard: AuthorityGuard<Error: 'static> + 'static {}
impl<G: AuthorityGuard<Error: 'static> + 'static> OwnedAuthorityGuard for G {}

#[cfg(not(target_arch = "wasm32"))]
type Reply<E> =
    Box<dyn FnOnce(Result<AppendGroupOutcome, AuthorityError<E>>, OwnedAccounting) + Send>;
#[cfg(target_arch = "wasm32")]
type Reply<E> = Box<dyn FnOnce(Result<AppendGroupOutcome, AuthorityError<E>>, OwnedAccounting)>;

/// Prepared authority requests, seals, and original accounting in one handoff.
/// Adapters forward this value; the canonical owner binds it to its append path.
pub struct OwnedAuthorityGroup<G: OwnedAuthorityGuard> {
    requests: Retained<Vec<GuardedAppend>>,
    cuts: Retained<Vec<PolicyCut>>,
    accounting: Option<OwnedAccounting>,
    guard: Option<G>,
    reply: Option<Retained<Reply<G::Error>>>,
}

/// The boxed operation is destroyed before its original frame funding.
pub struct OwnedAuthorityTask {
    future: crate::heap_future::OwnedFuture<dyn Future<Output = ()> + Send + 'static, ()>,
    _allocation: RetainedAllocation,
}

impl Future for OwnedAuthorityTask {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        self.get_mut().future.as_mut().poll(cx)
    }
}

impl OwnedAuthorityTask {
    pub(crate) fn admit<S, F, E>(
        state: S,
        admit: impl FnOnce(&mut S, u64) -> Result<RetainedAllocation, E>,
        create: impl FnOnce(S) -> F,
    ) -> Result<Self, E>
    where
        F: Future<Output = ()> + ProviderTask + 'static,
    {
        let mut allocation = None;
        let (future, _) = crate::heap_future::in_heap_admitted(
            state,
            |state, bytes| {
                allocation = Some(admit(state, bytes)?);
                Ok(())
            },
            create,
        )?;
        Ok(Self {
            future: future.erase_send(),
            _allocation: allocation
                .unwrap_or_else(|| unreachable!("frame admitted before construction")),
        })
    }
}

impl<G: OwnedAuthorityGuard> OwnedAuthorityGroup<G> {
    pub(crate) fn new<F>(
        requests: Retained<Vec<GuardedAppend>>,
        cuts: Retained<Vec<PolicyCut>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
        guard: Option<G>,
        reply: F,
    ) -> Result<Self, crate::FsError>
    where
        F: FnOnce(Result<AppendGroupOutcome, AuthorityError<G::Error>>, OwnedAccounting)
            + ProviderTask
            + 'static,
    {
        let allocation = RetainedAllocation::claim(std::mem::size_of::<F>() as u64, 1, accounting)?;
        let reply: Reply<G::Error> = Box::new(reply);
        let reply = Retained {
            value: reply,
            allocation,
        };
        let accounting = OwnedAccounting::take_branch(accounting, cancellation)?;
        Ok(Self {
            requests,
            cuts,
            accounting: Some(accounting),
            guard,
            reply: Some(reply),
        })
    }

    /// Runs against this owned provider through its existing atomic append API.
    ///
    /// An admission failure delivers its typed terminal reply and returns None.
    pub fn bind<A: AsyncAuthorityStore + 'static>(self, owner: A) -> Option<OwnedAuthorityTask> {
        OwnedAuthorityTask::admit(
            self,
            |batch, bytes| match RetainedAllocation::claim(
                bytes,
                u64::from(bytes != 0),
                &mut batch
                    .accounting
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("undispatched accounting"))
                    .borrow(),
            ) {
                Ok(credit) => Ok(credit),
                Err(error) => {
                    batch.finish(Err(super::work::authority_error(error).into()));
                    Err(())
                }
            },
            |batch| async move { batch.run(owner).await },
        )
        .ok()
    }

    fn finish(&mut self, result: Result<AppendGroupOutcome, AuthorityError<G::Error>>) {
        if let Some(Retained {
            value: reply,
            allocation,
        }) = self.reply.take()
        {
            reply(
                result,
                self.accounting
                    .take()
                    .unwrap_or_else(|| unreachable!("one terminal accounting return")),
            );
            drop(allocation);
        }
    }

    async fn run<A: AsyncAuthorityStore>(mut self, owner: A) {
        let cancellation = self
            .accounting
            .as_ref()
            .unwrap_or_else(|| unreachable!("running accounting"))
            .cancellation();
        let result = async {
            let mut accounting = self
                .accounting
                .as_mut()
                .unwrap_or_else(|| unreachable!("running accounting"))
                .borrow();
            let mut work = AuthorityWork::new(
                accounting
                    .remaining()
                    .map_err(super::work::authority_error)?,
                &cancellation,
            );
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                work = AuthorityWork::native(original, &cancellation);
            }
            let result = owner
                .compare_and_append_group(
                    &self.requests.value,
                    &self.cuts.value,
                    &mut work,
                    self.guard.as_mut(),
                )
                .await;
            let completed = work.spent();
            drop(work);
            if accounting.standalone_parts().is_some() {
                accounting
                    .accept_backend(completed)
                    .map_err(super::work::authority_error)?;
            }
            result
        }
        .await;
        self.finish(result);
    }
}

impl<G: OwnedAuthorityGuard> Drop for OwnedAuthorityGroup<G> {
    fn drop(&mut self) {
        if let Some(Retained {
            value: reply,
            allocation,
        }) = self.reply.take()
        {
            reply(
                Err(
                    crate::AuthorityStoreError::RejectedStatic("owned authority dispatch dropped")
                        .into(),
                ),
                self.accounting
                    .take()
                    .unwrap_or_else(|| unreachable!("one terminal accounting return")),
            );
            drop(allocation);
        }
    }
}
