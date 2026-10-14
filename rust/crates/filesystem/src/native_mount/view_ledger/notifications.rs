use super::*;
use crate::demand::NativeMountAllocationCustody;

#[derive(Clone)]
pub(super) struct ObserverEntry {
    pub(super) observer: Weak<dyn ViewObserver>,
    _custody: Option<Arc<NativeMountAllocationCustody>>,
}

pub(super) struct ObserverEntries {
    pub(super) entries: Vec<ObserverEntry>,
    custody: Arc<NativeMountAllocationCustody>,
}

impl ViewLedger {
    pub(in crate::native_mount) fn observe(
        self: &Arc<Self>,
        observer: Weak<dyn ViewObserver>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        let observer_custody = observer
            .upgrade()
            .and_then(|observer| observer.allocation_custody());
        let entry = ObserverEntry {
            observer,
            _custody: observer_custody,
        };
        let entry_bytes = std::mem::size_of_val(&entry);
        let mut hub = self
            .observers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let old = hub.clone();
        let entries = old.as_ref().map_or(&[][..], |old| old.entries.as_slice());
        storage::charge(carrier, scan_work(entries.len() as u64))?;
        let count = entries
            .iter()
            .filter(|entry| entry.observer.strong_count() != 0)
            .count()
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?;
        let borrowed = match &old {
            Some(old) => storage::adopt_backing(
                Some(&old.custody),
                (old.entries.capacity() * entry_bytes
                    + std::mem::size_of::<ObserverEntries>()
                    + 2 * std::mem::size_of::<usize>()) as u64,
                carrier,
            )?,
            None => 0,
        };
        let registered = (|| {
            let (mut next, custody) = storage::vector(count, carrier)?;
            let custody = custody.ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?;
            carrier
                .with_work(|work| {
                    custody.claim(
                        (std::mem::size_of::<ObserverEntries>() + 2 * std::mem::size_of::<usize>())
                            as u64,
                        work,
                    )
                })
                .map_err(super::super::lazy::mount_demand_error)?;
            storage::charge(carrier, copy_work((count * entry_bytes) as u64))?;
            next.extend(
                entries
                    .iter()
                    .filter(|entry| entry.observer.strong_count() != 0)
                    .cloned(),
            );
            next.push(entry);
            *hub = Some(Arc::new(ObserverEntries {
                entries: next,
                custody,
            }));
            Ok(())
        })();
        registered.and(storage::release_backing(borrowed, carrier))
    }

    pub(in crate::native_mount) fn notify_everything(&self) {
        let observers = self
            .observers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let position = ViewStamp::next();
        position.record_in(&self.everything);
        position.record_in(&self.latest);
        for observer in observers
            .iter()
            .flat_map(|entries| &entries.entries)
            .filter_map(|entry| entry.observer.upgrade())
        {
            observer.view_changed(position, ViewOrigin::current());
        }
    }
}
