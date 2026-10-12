impl<A, O> CheckoutMountSource<A, O> {
    fn binding_lease(
        &self,
        expected_epoch: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        let mut carrier = NativeDemandCarrier::Native(work);
        let mut lease = invoke(&self.runtime, work, || {
            self.checkout.view_gate.read_for_callback_with_carrier(
                ViewGate::callback_owner(),
                expected_epoch,
                &mut carrier,
                work.cancellation(),
                Some(&self.cancellation),
            )
        })?;
        let bytes = std::mem::size_of::<ViewReadLease>() as u64;
        carrier
            .with_work(|leaf| leaf.allocate(bytes))
            .map_err(super::lazy::mount_demand_error)?;
        if let Err(error) = lease.attach_owned_payload(bytes) {
            carrier
                .with_work(|leaf| leaf.release(bytes))
                .map_err(super::lazy::mount_demand_error)?;
            return Err(error);
        }
        Ok(Box::new(lease))
    }
}

impl NativeDemandCarrier<'_> {
    fn check(
        &mut self,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError> {
        cancellation
            .check()
            .map_err(|_| MountSourceError::Cancelled)?;
        if lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Err(MountSourceError::Stale);
        }
        match self.original_native_work() {
            Some(work) => work
                .admit(crate::WorkCounters::UNCHARGED)
                .map_err(MountSourceError::NativeWork),
            None => self
                .with_work(|work| work.check())
                .map_err(super::lazy::mount_demand_error),
        }
    }
}

struct LockWait<'a, 'w> {
    carrier: &'a mut NativeDemandCarrier<'w>,
    waits: [Option<crate::cancellation::PreparedCancelled>; 2],
    reserved: u64,
}

impl LockWait<'_, '_> {
    fn prepare(&mut self, index: usize, token: &CancellationToken) -> Result<(), MountSourceError> {
        let native = self.carrier.original_native_work();
        let wait = match &native {
            Some(work) => token.prepare_cancelled_with_native_work(work),
            None => self.carrier.with_work(|work| work.prepare_cancelled(token)),
        }
        .map_err(super::lazy::mount_wait_error)?;
        let cleanup = wait.cleanup_items();
        self.waits[index] = Some(wait);
        if native.is_none() {
            self.carrier
                .with_work(|work| {
                    let mut budget = work.budget;
                    budget.items_examined = budget
                        .items_examined
                        .checked_sub(cleanup)
                        .ok_or(crate::WorkError::Overflow)?;
                    work.work.verify(budget)?;
                    work.budget = budget;
                    self.reserved = self
                        .reserved
                        .checked_add(cleanup)
                        .ok_or(crate::WorkError::Overflow)?;
                    Ok::<_, crate::WorkError>(())
                })
                .map_err(MountSourceError::Work)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), MountSourceError> {
        let mut waits = std::mem::take(&mut self.waits);
        for wait in waits.iter_mut().flatten() {
            wait.retire_waker();
        }
        if self.carrier.original_native_work().is_some() {
            return Ok(());
        }
        let reserved = std::mem::take(&mut self.reserved);
        self.carrier.with_work(|work| {
            work.budget.items_examined = work
                .budget
                .items_examined
                .checked_add(reserved)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            let mut result = Ok(());
            for wait in waits.into_iter().flatten() {
                let finished = work
                    .finish_cancelled(wait)
                    .map_err(super::lazy::mount_wait_error);
                result = result.and(finished);
            }
            result
        })
    }
}

impl Drop for LockWait<'_, '_> {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

pub(super) async fn await_lock<F: Future>(
    lock: F,
    carrier: &mut NativeDemandCarrier<'_>,
    cancellation: &CancellationToken,
    lifetime: Option<&CancellationToken>,
) -> Result<F::Output, MountSourceError> {
    carrier.check(cancellation, lifetime)?;
    let mut waits = LockWait {
        carrier,
        waits: [None, None],
        reserved: 0,
    };
    let mut lock = std::pin::pin!(lock);
    let tokens = [
        Some(cancellation),
        lifetime.filter(|token| !token.same_state(cancellation)),
    ];
    let result = std::future::poll_fn(|context| {
        if let Poll::Ready(guard) = lock.as_mut().poll(context) {
            return Poll::Ready(Ok(guard));
        }
        for (index, token) in tokens.iter().enumerate() {
            let Some(token) = token else {
                continue;
            };
            let error = if index == 0 {
                MountSourceError::Cancelled
            } else {
                MountSourceError::Stale
            };
            if token.is_cancelled() {
                return Poll::Ready(Err(error));
            }
            if waits.waits[index].is_none() {
                if let Err(error) = waits.prepare(index, token) {
                    return Poll::Ready(Err(error));
                }
            }
            if Pin::new(waits.waits[index].as_mut().expect("prepared cancellation"))
                .poll(context)
                .is_ready()
            {
                return Poll::Ready(Err(error));
            }
        }
        Poll::Pending
    })
    .await;
    let finished = waits.finish();
    let guard = result?;
    finished?;
    waits.carrier.check(cancellation, lifetime)?;
    Ok(guard)
}
