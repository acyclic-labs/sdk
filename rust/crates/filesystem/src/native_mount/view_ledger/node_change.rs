use super::*;
use crate::demand::NativeMountAllocationCustody;

pub(in crate::native_mount) struct PreparedNativeNodeChange<'a> {
    ledger: &'a ViewLedger,
    slot: storage::PreparedSlot<'a>,
    observers: (Vec<Arc<dyn ViewObserver>>, Option<NativeMountAllocationCustody>),
    completion: crate::materializer::NativeWorkReservation,
}

impl ViewLedger {
    pub(in crate::native_mount) fn prepare_node_change(
        &self, file_id: FileId, original: &crate::NativeMaterializationWork,
    ) -> Result<PreparedNativeNodeChange<'_>, MountSourceError> {
        let prepared = (|| {
        let mut carrier = NativeDemandCarrier::Native(original);
        let observers = self.observers.live(&mut carrier)?;
        storage::charge(&mut carrier, crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let slot = self.nodes.prepare(key_of(&file_id), &mut carrier)?;
        let mut maximum = slot.work()?;
        maximum.items_examined = maximum.items_examined.checked_add(observers.0.len() as u64)
            .ok_or(crate::WorkError::Overflow)?;
        let completion = original.reserve_work(maximum).map_err(MountSourceError::NativeWork)?;
        Ok(PreparedNativeNodeChange { ledger: self, slot, observers, completion })
        })();
        prepared.map_err(|error| match error {
            MountSourceError::Work(error) => MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(error)),
            MountSourceError::Cancelled => MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled),
            MountSourceError::Allocation(error) => MountSourceError::NativeWork(crate::NativeTreeMaterializationError::AllocationFailed(error)),
            error => error,
        })
    }

    pub(in crate::native_mount) fn record_node_change_prepared(&self, prepared: PreparedNativeNodeChange<'_>)
        -> Result<(), MountSourceError>
    {
        let PreparedNativeNodeChange { ledger, slot, observers, mut completion } = prepared;
        assert!(std::ptr::eq(self, ledger), "original prepared node ledger");
        let actual = slot.work().expect("prepared slot fits original reservation");
        let position = ViewStamp::next();
        slot.record(position);
        position.record_in(&self.latest);
        let mut result = completion.record_completed(actual).map_err(MountSourceError::NativeWork);
        // Release the map lock before reentrant observers and retire before unwinding.
        let notified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            for observer in &observers.0 {
                let visit = completion.record_completed(crate::WorkCounters { items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork);
                if result.is_ok() { result = visit; }
                observer.view_changed(position, ViewOrigin::current());
            }
        }));
        drop(observers);
        drop(completion);
        if let Err(panic) = notified { std::panic::resume_unwind(panic); }
        result
    }
}
