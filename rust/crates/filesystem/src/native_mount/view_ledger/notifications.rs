use super::*;
use crate::demand::{NativeDemandReadMeter, NativeMountAllocationCustody};
use crate::materializer::NativeWorkReservation;

#[derive(Default)]
pub(super) struct ViewObservers(Mutex<Observers>);

#[derive(Default)]
struct Observers {
    entries: Vec<(Weak<dyn ViewObserver>, Option<NativeMountAllocationCustody>)>,
    custody: Option<NativeMountAllocationCustody>,
    pending: Option<PendingNotification>,
}

struct PendingNotification {
    live: Vec<Arc<dyn ViewObserver>>,
    custody: Option<NativeMountAllocationCustody>,
    completion: NativeWorkReservation,
}

pub(in crate::native_mount) struct PreparedEverythingNotification(Option<Arc<ViewLedger>>);

impl Drop for PreparedEverythingNotification {
    fn drop(&mut self) {
        if let Some(ledger) = self.0.take() {
            let pending = ledger
                .observers
                .0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .pending
                .take();
            drop(pending);
        }
    }
}

impl ViewObservers {
    pub(super) fn live(
        &self,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<
        (
            Vec<Arc<dyn ViewObserver>>,
            Option<NativeMountAllocationCustody>,
        ),
        MountSourceError,
    > {
        let observers = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        storage::charge(
            carrier,
            crate::WorkCounters {
                items_examined: observers.entries.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            },
        )?;
        let count = observers
            .entries
            .iter()
            .filter(|(observer, _)| observer.strong_count() != 0)
            .count();
        let mut live = storage::vector(count, carrier)?;
        for (observer, _) in &observers.entries {
            storage::charge(
                carrier,
                crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                },
            )?;
            if let Some(observer) = observer.upgrade() {
                storage::charge(
                    carrier,
                    crate::WorkCounters {
                        bytes_copied: std::mem::size_of::<Arc<dyn ViewObserver>>() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    },
                )?;
                live.0.push(observer);
            }
        }
        Ok(live)
    }
}

impl ViewLedger {
    pub(in crate::native_mount) fn observe(
        self: &Arc<Self>,
        observer: Weak<dyn ViewObserver>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        let custody = observer
            .upgrade()
            .and_then(|observer| observer.allocation_custody());
        loop {
            let original = self
                .observers
                .0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .pending
                .as_mut()
                .map(|pending| pending.completion.work().clone());
            let mut credit = original
                .as_ref()
                .map(|work| {
                    work.reserve_work(crate::WorkCounters {
                        items_examined: 2,
                        bytes_copied: std::mem::size_of::<Arc<dyn ViewObserver>>() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    })
                })
                .transpose()
                .map_err(MountSourceError::NativeWork)?;
            let mut hub = self
                .observers
                .0
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let current = hub
                .pending
                .as_mut()
                .map(|pending| pending.completion.work().clone());
            if current
                .as_ref()
                .map(|work| Arc::as_ptr(&work.state))
                != original
                    .as_ref()
                    .map(|work| Arc::as_ptr(&work.state))
            {
                drop(hub);
                drop(credit);
                storage::charge(
                    carrier,
                    crate::WorkCounters {
                        items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED
                    },
                )?;
                continue;
            }
            storage::charge(
                carrier,
                crate::WorkCounters {
                    items_examined: hub.entries.len() as u64,
                    ..crate::WorkCounters::UNCHARGED
                },
            )?;
            hub.entries
                .retain(|(observer, _)| observer.strong_count() != 0);
            if hub.entries.len() == hub.entries.capacity() {
                let capacity = hub
                    .entries
                    .capacity()
                    .max(1)
                    .checked_mul(2)
                    .ok_or(crate::WorkError::Overflow)?;
                let Observers {
                    entries, custody, ..
                } = &mut *hub;
                storage::grow_vector(entries, custody, capacity, carrier)?;
            }
            let count = hub
                .entries
                .len()
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?;
            if let Some(pending) = &mut hub.pending {
                if pending.live.capacity() < count {
                    storage::grow_vector(&mut pending.live, &mut pending.custody, count, carrier)?;
                }
            }
            storage::charge(
                carrier,
                crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<(
                        Weak<dyn ViewObserver>,
                        Option<NativeMountAllocationCustody>,
                    )>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                },
            )?;
            if let Some(pending) = &mut hub.pending {
                pending
                    .completion
                    .absorb(credit.as_mut().expect("same pending publication"))
                    .map_err(MountSourceError::NativeWork)?;
            }
            hub.entries.push((observer, custody));
            return Ok(());
        }
    }

    pub(in crate::native_mount) fn prepare_everything_notification(
        self: &Arc<Self>,
        original: &Arc<NativeDemandReadMeter>,
    ) -> Result<PreparedEverythingNotification, MountSourceError> {
        let mut hub = self
            .observers
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if hub.pending.is_some() {
            return Err(MountSourceError::Stale);
        }
        let mut carrier = NativeDemandCarrier::Shared(original);
        storage::charge(
            &mut carrier,
            crate::WorkCounters {
                items_examined: hub.entries.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            },
        )?;
        hub.entries
            .retain(|(observer, _)| observer.strong_count() != 0);
        let count = hub.entries.len();
        let completion = crate::NativeMaterializationWork::from_meter(Arc::clone(original))
            .reserve_work(crate::WorkCounters {
                items_examined: (count as u64)
                    .checked_mul(2)
                    .ok_or(crate::WorkError::Overflow)?,
                bytes_copied: (count as u64)
                    .checked_mul(std::mem::size_of::<Arc<dyn ViewObserver>>() as u64)
                    .ok_or(crate::WorkError::Overflow)?,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(MountSourceError::NativeWork)?;
        let live = storage::vector(count, &mut carrier)?;
        hub.pending = Some(PendingNotification {
            live: live.0,
            custody: live.1,
            completion,
        });
        Ok(PreparedEverythingNotification(Some(Arc::clone(self))))
    }
}

impl PreparedEverythingNotification {
    pub(in crate::native_mount) fn record(mut self) {
        let ledger = self.0.take().expect("prepared publication");
        let (mut pending, position) = {
            let mut hub = ledger
                .observers
                .0
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let mut pending = hub.pending.take().expect("original notification");
            for (observer, _) in &hub.entries {
                let live = observer.upgrade();
                pending
                    .completion
                    .record_completed(crate::WorkCounters {
                        items_examined: 1,
                        bytes_copied: if live.is_some() {
                            std::mem::size_of::<Arc<dyn ViewObserver>>() as u64
                        } else {
                            0
                        },
                        ..crate::WorkCounters::UNCHARGED
                    })
                    .expect("publication owns its delivery credit");
                if let Some(observer) = live {
                    pending.live.push(observer);
                }
            }
            let position = ViewStamp::next();
            position.record_in(&ledger.everything);
            position.record_in(&ledger.latest);
            (pending, position)
        };
        for observer in &pending.live {
            pending
                .completion
                .record_completed(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })
                .expect("publication owns its delivery credit");
            observer.view_changed(position, ViewOrigin::current());
        }
    }
}
