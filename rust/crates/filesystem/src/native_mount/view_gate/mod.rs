use super::MountSourceError;
use crate::lock_order::{GATE, Mutex as RankedMutex};
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};
use tokio::sync::Notify;

mod reads;
use reads::{ReadOwner, ReaderIndex};
mod leases;
use leases::WaitingWriter;
pub(super) use leases::{ViewMutationPhase, ViewReadLease, ViewWriteLease};
#[cfg(test)]
mod tests;

/// Internal callback reentrancy identity, never an OS or authority capability.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct CallbackOwner(u64);

static NEXT_CALLBACK_OWNER: AtomicU64 = AtomicU64::new(1);
thread_local! {
    static CALLBACK_OWNER: Cell<u64> = const { Cell::new(0) };
}

#[derive(Default)]
struct ViewState {
    readers: usize,
    owners: ReaderIndex,
    writer: bool,
    mutation_phase: bool,
    waiting_writers: usize,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum ViewReaderId {
    Callback(CallbackOwner),
    #[cfg(test)]
    Independent(u64),
}

/// Pins a coherent mount view across a native callback or authored mutation.
pub(super) struct ViewGate {
    state: RankedMutex<ViewState, GATE>,
    changed: Notify,
    generation: AtomicU64,
    #[cfg(test)]
    next_reader: AtomicU64,
    // The gate's readers and wake state drop before its original custody.
    _native_allocation: Option<crate::NativeAllocation>,
}

impl ViewGate {
    pub(super) const INITIAL_GENERATION: u64 = 2;

    pub(super) fn new() -> Self {
        Self {
            state: RankedMutex::new(ViewState::default()),
            changed: Notify::new(),
            generation: AtomicU64::new(Self::INITIAL_GENERATION),
            #[cfg(test)]
            next_reader: AtomicU64::new(1),
            _native_allocation: None,
        }
    }

    pub(super) fn new_with_native_work(
        original: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, MountSourceError> {
        let allocation = original
            .claim(
                std::mem::size_of::<Self>() as u64,
                1,
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(MountSourceError::NativeWork)?;
        let mut value = Self::new();
        value._native_allocation = Some(allocation);
        Ok(Arc::new(value))
    }

    #[cfg(test)]
    pub(super) async fn read(
        self: &Arc<Self>,
        expected: Option<u64>,
    ) -> Result<ViewReadLease, MountSourceError> {
        let owner = ViewReaderId::Independent(self.next_reader.fetch_add(1, Ordering::Relaxed));
        self.read_as(owner, expected).await
    }

    /// Whether `owner`'s callback already holds a read of the view, which a
    /// writer it waited for would never see released.
    pub(super) fn reads_for_callback(&self, owner: CallbackOwner) -> bool {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match state
            .owners
            .get(ViewReaderId::Callback(owner).key(), &mut reads::OwnerLookup)
        {
            Ok(owner) => owner.is_some(),
            Err(impossible) => match impossible {},
        }
    }

    pub(super) fn callback_owner() -> Result<CallbackOwner, MountSourceError> {
        CALLBACK_OWNER.with(|owner| {
            let mut id = owner.get();
            if id == 0 {
                id = NEXT_CALLBACK_OWNER
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                    .map_err(|_| MountSourceError::Work(crate::WorkError::Overflow))?;
                owner.set(id);
            }
            Ok(CallbackOwner(id))
        })
    }

    pub(super) async fn read_for_callback(
        self: &Arc<Self>,
        owner: CallbackOwner,
        expected: Option<u64>,
    ) -> Result<ViewReadLease, MountSourceError> {
        self.read_as(ViewReaderId::Callback(owner), expected).await
    }

    async fn read_as(
        self: &Arc<Self>,
        owner: ViewReaderId,
        expected: Option<u64>,
    ) -> Result<ViewReadLease, MountSourceError> {
        let cancellation = crate::CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let mut carrier = super::adapter::NativeDemandCarrier::Borrowed(&mut work);
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(lease) = self.try_read_as(owner, expected, &mut carrier)? {
                return Ok(lease);
            }
            notified.await;
        }
    }

    pub(super) async fn read_for_callback_with_carrier(
        self: &Arc<Self>,
        owner: CallbackOwner,
        expected: Option<u64>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken,
        lifetime: Option<&crate::CancellationToken>,
    ) -> Result<ViewReadLease, MountSourceError> {
        loop {
            if cancellation.is_cancelled() {
                return Err(MountSourceError::Cancelled);
            }
            if lifetime.is_some_and(crate::CancellationToken::is_cancelled) {
                return Err(MountSourceError::Stale);
            }
            carrier
                .with_work(|work| work.check())
                .map_err(MountSourceError::from)?;
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let admitted = self.try_read_as(ViewReaderId::Callback(owner), expected, carrier)?;
            if let Some(lease) = admitted {
                return Ok(lease);
            }
            super::adapter::await_with_cancellation(notified, carrier, cancellation, lifetime)
                .await?;
        }
    }

    pub(super) async fn write(self: &Arc<Self>) -> ViewWriteLease {
        self.acquire_writer().await
    }

    pub(super) async fn write_stable(
        self: &Arc<Self>,
        expected: Option<u64>,
    ) -> Result<ViewWriteLease, MountSourceError> {
        let lease = self.acquire_writer().await;
        if !lease.generation.is_multiple_of(2)
            || expected.is_some_and(|expected| expected != lease.generation)
        {
            return Err(MountSourceError::Stale);
        }
        Ok(lease)
    }

    pub(super) async fn write_stable_with_carrier(
        self: &Arc<Self>,
        expected: Option<u64>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken,
        lifetime: Option<&crate::CancellationToken>,
    ) -> Result<ViewWriteLease, MountSourceError> {
        carrier
            .with_work(|work| work.check())
            .map_err(MountSourceError::from)?;
        super::adapter::await_with_cancellation(
            self.write_stable(expected),
            carrier,
            cancellation,
            lifetime,
        )
        .await?
    }

    async fn acquire_writer(self: &Arc<Self>) -> ViewWriteLease {
        let mut waiter = WaitingWriter::register(self);
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                if !state.writer && !state.mutation_phase && state.readers == 0 {
                    let generation = self.generation.load(Ordering::Acquire);
                    waiter.admit(&mut state);
                    state.writer = true;
                    return ViewWriteLease {
                        generation,
                        gate: Arc::clone(self),
                        armed: true,
                    };
                }
            }
            notified.await;
        }
    }

    pub(super) fn begin_transition(&self) {
        let generation = self.generation.load(Ordering::Acquire);
        if generation.is_multiple_of(2) {
            self.generation.store(generation + 1, Ordering::Release);
        }
    }

    pub(super) fn finish_transition(&self) {
        let generation = self.generation.load(Ordering::Acquire);
        debug_assert_eq!(generation % 2, 1);
        self.generation.store(generation + 1, Ordering::Release);
    }

    pub(super) fn is_stable(&self) -> bool {
        self.generation.load(Ordering::Acquire).is_multiple_of(2)
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}
