use super::*;
use crate::demand::NativeMountAllocationCustody;
use std::sync::RwLockWriteGuard;

pub(in crate::native_mount) struct PreparedNativeNodeChange<'a> {
    ledger: &'a ViewLedger,
    positions: RwLockWriteGuard<'a, storage::ChangePositions>,
    key: u64,
    observers: (
        Vec<Arc<dyn ViewObserver>>,
        Option<NativeMountAllocationCustody>,
    ),
    completion: crate::materializer::NativeWorkReservation,
}

impl ViewLedger {
    pub(in crate::native_mount) fn prepare_node_change(
        &self,
        file_id: FileId,
        original: &crate::NativeMaterializationWork,
    ) -> Result<PreparedNativeNodeChange<'_>, MountSourceError> {
        let prepared = (|| {
            let mut carrier = NativeDemandCarrier::Native(original);
            let observers = self.observers.live(&mut carrier)?;
            storage::charge(
                &mut carrier,
                crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                },
            )?;
            let key = key_of(&file_id);
            let positions = self.nodes.prepare(key, &mut carrier)?;
            let mut maximum = positions.work(key)?;
            maximum.items_examined = maximum
                .items_examined
                .checked_add(observers.0.len() as u64)
                .ok_or(crate::WorkError::Overflow)?;
            let completion = original
                .reserve_work(maximum)
                .map_err(MountSourceError::NativeWork)?;
            Ok(PreparedNativeNodeChange {
                ledger: self,
                positions,
                key,
                observers,
                completion,
            })
        })();
        prepared.map_err(|error| match error {
            MountSourceError::Work(error) => {
                MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(error))
            }
            MountSourceError::Cancelled => {
                MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled)
            }
            MountSourceError::Allocation(error) => MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::AllocationFailed(error),
            ),
            error => error,
        })
    }
}

impl PreparedNativeNodeChange<'_> {
    pub(in crate::native_mount) fn record(self) -> Result<(), MountSourceError> {
        let Self {
            ledger,
            mut positions,
            key,
            observers,
            mut completion,
        } = self;
        let actual = positions
            .work(key)
            .expect("held positions fit the original reservation");
        let position = ViewStamp::next();
        positions.record(key, position);
        drop(positions);
        position.record_in(&ledger.latest);
        let mut result = completion
            .record_completed(actual)
            .map_err(MountSourceError::NativeWork);
        for observer in &observers.0 {
            let visit = completion
                .record_completed(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })
                .map_err(MountSourceError::NativeWork);
            if result.is_ok() {
                result = visit;
            }
            observer.view_changed(position, ViewOrigin::current());
        }
        result
    }
}
