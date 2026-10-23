use super::*;

/// A genuine prepared Commit-family ingress.
pub struct PreparedNativeCommit {
    future: NativeProviderFuture<'static, CommitOutcome>,
    recovery: Option<NativeProviderFuture<'static, Option<IdempotencyObservation>>>,
    record: Arc<NativeCommitRecordSlot>,
    maximum_record_bytes: usize,
}

impl PreparedNativeCommit {
    pub(crate) fn new(
        future: NativeProviderFuture<'static, CommitOutcome>,
        recovery: NativeProviderFuture<'static, Option<IdempotencyObservation>>,
        record: Arc<NativeCommitRecordSlot>,
        maximum_record_bytes: usize,
    ) -> Self {
        Self {
            future,
            recovery: Some(recovery),
            record,
            maximum_record_bytes,
        }
    }

    /// Transfers the already funded same-key inspection future exactly once.
    pub fn take_recovery_future(
        &mut self,
    ) -> Option<NativeProviderFuture<'static, Option<IdempotencyObservation>>> {
        self.recovery.take()
    }

    /// Supplies the unchanged canonical request's one record and transfers its already funded future.
    pub fn finish(
        mut self,
        record: crate::AppendRecord,
    ) -> NativeProviderFuture<'static, CommitOutcome> {
        let record = if record.value.is_empty() {
            Err(StreamError::InvalidArgument)
        } else if record.payload_bytes() > self.maximum_record_bytes {
            Err(StreamError::LimitExceeded)
        } else {
            crate::changes::validate(&record.value, &record.headers).map(|()| record)
        };
        self.record.supply(record);
        std::mem::replace(
            &mut self.future,
            NativeProviderFuture::ready(Ok(Err(StreamError::Unavailable))),
        )
    }
}

impl Drop for PreparedNativeCommit {
    fn drop(&mut self) {
        self.record.close();
    }
}

type RecordSlotState = (
    Option<Result<crate::AppendRecord, StreamError>>,
    Option<std::task::Waker>,
);

/// One private ingress cell, not a journal, authority or request queue.
pub(crate) struct NativeCommitRecordSlot {
    state: std::sync::Mutex<RecordSlotState>,
    _allocation: acyclic_native_runtime::ReadAllocation,
}

impl NativeCommitRecordSlot {
    pub(crate) fn new(
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Arc<Self>> {
        let bytes = std::mem::size_of::<Self>();
        let allocation = acyclic_native_runtime::ReadAllocation::new(meter, bytes)?;
        meter.admit_codec(0, bytes as u64, 0)?;
        let record = Arc::new(Self {
            state: std::sync::Mutex::new((None, None)),
            _allocation: allocation,
        });
        meter.record_codec(0, bytes as u64, 0)?;
        meter.check()?;
        Ok(record)
    }

    pub(crate) async fn receive(&self) -> Result<crate::AppendRecord, StreamError> {
        futures::future::poll_fn(|context| {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(record) = &mut state.0 {
                return std::task::Poll::Ready(std::mem::replace(
                    record,
                    Err(StreamError::Unavailable),
                ));
            }
            if !state
                .1
                .as_ref()
                .is_some_and(|waiter| waiter.will_wake(context.waker()))
            {
                state.1 = Some(context.waker().clone());
            }
            std::task::Poll::Pending
        })
        .await
    }

    fn supply(&self, record: Result<crate::AppendRecord, StreamError>) {
        let waiter = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.0 = Some(record);
            state.1.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    pub(crate) fn close(&self) {
        let waiter = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.0.get_or_insert(Err(StreamError::Unavailable));
            state.1.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}
