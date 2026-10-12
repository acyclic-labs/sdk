use super::*;
use crate::demand::{NativeDemandReadMeter, NativeMountAllocationCustody};
use crate::materializer::NativeResourceCredit;
use super::super::lazy::mount_demand_error;

#[derive(Default)]
pub(super) struct ViewObservers(Mutex<Observers>);

#[derive(Default)]
struct Observers {
    entries: Vec<(Weak<dyn ViewObserver>, Option<NativeMountAllocationCustody>)>,
    custody: Option<NativeMountAllocationCustody>,
    pending: Option<PendingNotification>,
}

struct PendingNotification {
    entries: Vec<Delivery>,
    custody: Option<NativeMountAllocationCustody>,
    original: Arc<NativeDemandReadMeter>,
}

enum Recipient {
    Waiting(Weak<dyn ViewObserver>),
    Ready(Option<Arc<dyn ViewObserver>>),
}

struct Delivery {
    observer: Recipient,
    original: Arc<NativeDemandReadMeter>,
    credits: u64,
}

impl Delivery {
    fn visit(&mut self) {
        self.original.lock().complete(NativeResourceCredit::Items, 1, 1)
            .expect("actual notification owns an original credit");
        self.credits -= 1;
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        if self.credits != 0 {
            self.original.lock().refund(NativeResourceCredit::Items, self.credits)
                .expect("original notification credits balance");
        }
    }
}

pub(in crate::native_mount) struct PreparedEverythingNotification {
    ledger: Option<Arc<ViewLedger>>,
}

impl Drop for PreparedEverythingNotification {
    fn drop(&mut self) {
        if let Some(ledger) = self.ledger.take() {
            let pending = ledger.observers.0.lock().unwrap_or_else(PoisonError::into_inner).pending.take();
            drop(pending);
        }
    }
}

impl ViewObservers {
    pub(super) fn live(&self, carrier: &mut NativeDemandCarrier<'_>)
        -> Result<(Vec<Arc<dyn ViewObserver>>, Option<NativeMountAllocationCustody>), MountSourceError>
    {
        let observers = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let mut count = 0;
        for (observer, _) in &observers.entries {
            storage::charge(carrier, crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if observer.strong_count() != 0 { count += 1; }
        }
        let mut live = storage::vector(count, carrier)?;
        for (observer, _) in &observers.entries {
            storage::charge(carrier, crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if let Some(observer) = observer.upgrade() {
                storage::charge(carrier, crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<Arc<dyn ViewObserver>>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                live.0.push(observer);
            }
        }
        Ok(live)
    }
}

impl ViewLedger {
    pub(in crate::native_mount) fn observe(self: &Arc<Self>, observer: Weak<dyn ViewObserver>, carrier: &mut NativeDemandCarrier<'_>)
        -> Result<(), MountSourceError>
    {
        let custody = observer.upgrade().and_then(|observer| observer.allocation_custody());
        loop {
            let original = self.observers.0.lock().unwrap_or_else(PoisonError::into_inner)
                .pending.as_ref().map(|pending| Arc::clone(&pending.original));
            if let Some(original) = &original {
                original.lock().reserve(NativeResourceCredit::Items, 2).map_err(mount_demand_error)?;
            }
            let mut transferred = false;
            let result: Result<bool, MountSourceError> = (|| {
                let mut hub = self.observers.0.lock().unwrap_or_else(PoisonError::into_inner);
                let current = hub.pending.as_ref().map(|pending| &pending.original);
                if !match (current, &original) { (Some(a), Some(b)) => Arc::ptr_eq(a, b), (None, None) => true, _ => false } {
                    return Ok(false);
                }
                storage::charge(carrier, crate::WorkCounters { items_examined: hub.entries.len() as u64, ..crate::WorkCounters::UNCHARGED })?;
                hub.entries.retain(|(observer, _)| observer.strong_count() != 0);
                if hub.entries.len() == hub.entries.capacity() {
                    let capacity = hub.entries.capacity().max(1).checked_mul(2).ok_or(crate::WorkError::Overflow)?;
                    let Observers { entries, custody, .. } = &mut *hub;
                    storage::grow_vector(entries, custody, capacity, carrier)?;
                }
                storage::charge(carrier, crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<(Weak<dyn ViewObserver>, Option<NativeMountAllocationCustody>)>() as u64
                        + if hub.pending.is_some() { std::mem::size_of::<Delivery>() as u64 } else { 0 },
                    ..crate::WorkCounters::UNCHARGED
                })?;
                if let Some(pending) = &mut hub.pending {
                    if pending.entries.len() == pending.entries.capacity() {
                        let capacity = pending.entries.capacity().max(1).checked_mul(2).ok_or(crate::WorkError::Overflow)?;
                        storage::grow_vector(&mut pending.entries, &mut pending.custody, capacity, carrier)?;
                    }
                    pending.entries.push(Delivery { observer: Recipient::Waiting(observer.clone()),
                        original: Arc::clone(&pending.original), credits: 2 });
                    transferred = true;
                }
                hub.entries.push((observer.clone(), custody.clone()));
                Ok(true)
            })();
            if !transferred && let Some(original) = &original {
                original.lock().refund(NativeResourceCredit::Items, 2).expect("untransferred reservation balances");
            }
            match result? { true => return Ok(()), false => storage::charge(carrier,
                crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })? }
        }
    }

    pub(in crate::native_mount) fn prepare_everything_notification(self: &Arc<Self>, original: &Arc<NativeDemandReadMeter>)
        -> Result<PreparedEverythingNotification, MountSourceError>
    {
        // Match registration's hub-before-allocation order.
        let mut hub = self.observers.0.lock().unwrap_or_else(PoisonError::into_inner);
        if hub.pending.is_some() { return Err(MountSourceError::Stale); }
        let mut carrier = NativeDemandCarrier::Shared(original);
        storage::charge(&mut carrier, crate::WorkCounters { items_examined: hub.entries.len() as u64, ..crate::WorkCounters::UNCHARGED })?;
        let count = hub.entries.iter().filter(|(observer, _)| observer.strong_count() != 0).count();
        let entries = storage::vector(count, &mut carrier)?;
        let mut pending = PendingNotification { entries: entries.0, custody: entries.1, original: Arc::clone(original) };
        for (observer, _) in &hub.entries {
            storage::charge(&mut carrier, crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if observer.strong_count() == 0 { continue; }
            storage::charge(&mut carrier, crate::WorkCounters { bytes_copied: std::mem::size_of::<Delivery>() as u64,
                ..crate::WorkCounters::UNCHARGED })?;
            original.lock().reserve(NativeResourceCredit::Items, 2).map_err(mount_demand_error)?;
            pending.entries.push(Delivery { observer: Recipient::Waiting(observer.clone()), original: Arc::clone(original), credits: 2 });
        }
        hub.pending = Some(pending);
        Ok(PreparedEverythingNotification { ledger: Some(Arc::clone(self)) })
    }

    pub(in crate::native_mount) fn record_everything_prepared(&self, mut prepared: PreparedEverythingNotification) {
        let ledger = prepared.ledger.take().expect("prepared publication");
        assert!(std::ptr::eq(self, Arc::as_ptr(&ledger)), "original publication ledger");
        let (mut pending, position) = {
            let mut hub = self.observers.0.lock().unwrap_or_else(PoisonError::into_inner);
            let mut pending = hub.pending.take().expect("original notification");
            for delivery in &mut pending.entries {
                let Recipient::Waiting(observer) = &delivery.observer else { unreachable!("publication retains weak observers") };
                delivery.observer = Recipient::Ready(observer.upgrade());
            }
            let position = ViewStamp::next();
            position.record_in(&self.everything);
            position.record_in(&self.latest);
            (pending, position)
        };
        for delivery in &mut pending.entries {
            delivery.visit();
            if let Recipient::Ready(Some(observer)) = std::mem::replace(&mut delivery.observer, Recipient::Ready(None)) {
                delivery.visit();
                observer.view_changed(position, ViewOrigin::current());
            }
        }
    }
}
