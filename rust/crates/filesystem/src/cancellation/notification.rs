use super::*;

struct CancellationNotification<F> {
    notify: F,
    _allocation: Option<crate::NativeAllocation>,
}

impl<F: Fn() + Send + Sync + 'static> std::task::Wake for CancellationNotification<F> {
    fn wake(self: Arc<Self>) {
        (self.notify)();
    }
}

impl CancellationToken {
    /// Retains an O(1) registration on the token's existing waiter list.
    ///
    /// Register before acquiring the condition's mutex. The callback must take
    /// that mutex before notifying, and the waiter must check cancellation
    /// under the mutex immediately before waiting. This makes cancellation
    /// atomic with releasing the mutex into the OS wait.
    pub(crate) fn on_cancel<F: Fn() + Send + Sync + 'static>(
        &self,
        original: Option<&crate::NativeMaterializationWork>,
        notify: F,
    ) -> Result<Cancelled<'static>, crate::NativeTreeMaterializationError> {
        let mut registered = self.cancelled();
        registered
            .register(original)
            .map_err(CancellationWaitError::into_native)?;
        let allocation = original
            .map(|original| {
                original.claim(
                    std::mem::size_of::<CancellationNotification<F>>() as u64,
                    1,
                    crate::WorkCounters::UNCHARGED,
                )
            })
            .transpose()?;
        let waker = Waker::from(Arc::new(CancellationNotification {
            notify,
            _allocation: allocation,
        }));
        if Pin::new(&mut registered)
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
        {
            waker.wake();
        }
        Ok(registered.detach_meter())
    }
}
