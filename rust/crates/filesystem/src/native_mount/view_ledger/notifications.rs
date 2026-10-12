use super::*;
use crate::demand::NativeMountAllocationCustody;
use crate::materializer::NativeWorkReservation;

#[derive(Default)]
pub(super) struct Observers {
    pub(super) entries: Vec<(Weak<dyn ViewObserver>, Option<NativeMountAllocationCustody>)>,
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
            drop(
                ledger
                    .observers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .pending
                    .take(),
            );
        }
    }
}

impl Observers {
    pub(super) fn work(&self) -> Result<crate::WorkCounters, MountSourceError> {
        let count = self.entries.len() as u64;
        Ok(crate::WorkCounters {
            items_examined: count.checked_mul(2).ok_or(crate::WorkError::Overflow)?,
            bytes_copied: count
                .checked_mul(std::mem::size_of::<Arc<dyn ViewObserver>>() as u64)
                .ok_or(crate::WorkError::Overflow)?,
            ..crate::WorkCounters::UNCHARGED
        })
    }

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
        storage::charge(carrier, scan_work(self.entries.len() as u64))?;
        let count = self
            .entries
            .iter()
            .filter(|(observer, _)| observer.strong_count() != 0)
            .count();
        let mut live = storage::vector(count, carrier)?;
        for (observer, _) in &self.entries {
            storage::charge(carrier, scan_work(1))?;
            if let Some(observer) = observer.upgrade() {
                storage::charge(
                    carrier,
                    copy_work(std::mem::size_of::<Arc<dyn ViewObserver>>() as u64),
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
        let mut hub = self
            .observers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut credit = hub
            .pending
            .as_ref()
            .map(|pending| {
                pending.completion.work().reserve_work(crate::WorkCounters {
                    items_examined: 2,
                    bytes_copied: std::mem::size_of::<Arc<dyn ViewObserver>>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                })
            })
            .transpose()
            .map_err(MountSourceError::NativeWork)?;
        storage::charge(carrier, scan_work(hub.entries.len() as u64))?;
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
            copy_work(std::mem::size_of::<(
                Weak<dyn ViewObserver>,
                Option<NativeMountAllocationCustody>,
            )>() as u64),
        )?;
        if let Some(pending) = &mut hub.pending {
            pending
                .completion
                .absorb(credit.as_mut().expect("pending publication credit"))
                .map_err(MountSourceError::NativeWork)?;
        }
        hub.entries.push((observer, custody));
        Ok(())
    }

    pub(in crate::native_mount) fn prepare_everything_notification(
        self: &Arc<Self>,
        original: &crate::NativeMaterializationWork,
    ) -> Result<PreparedEverythingNotification, MountSourceError> {
        let mut hub = self
            .observers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if hub.pending.is_some() {
            return Err(MountSourceError::Stale);
        }
        storage::charge(
            &mut NativeDemandCarrier::Native(original),
            scan_work(hub.entries.len() as u64),
        )?;
        hub.entries
            .retain(|(observer, _)| observer.strong_count() != 0);
        let completion = original
            .reserve_work(hub.work()?)
            .map_err(MountSourceError::NativeWork)?;
        let (live, custody) = storage::vector(
            hub.entries.len(),
            &mut NativeDemandCarrier::Native(original),
        )?;
        hub.pending = Some(PendingNotification {
            live,
            custody,
            completion,
        });
        Ok(PreparedEverythingNotification(Some(Arc::clone(self))))
    }
}

impl PreparedEverythingNotification {
    pub(in crate::native_mount) fn record(mut self) {
        let ledger = self.0.take().expect("prepared publication");
        let (pending, position) = {
            let mut hub = ledger
                .observers
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
                .record_completed(scan_work(1))
                .expect("publication owns its delivery credit");
            observer.view_changed(position, ViewOrigin::current());
        }
    }
}
