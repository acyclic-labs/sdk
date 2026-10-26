use super::*;

impl CoreLog {
    pub(super) fn adopt_native(
        &mut self,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.residents().adopt_native(work)?;
        let needed = self.dirty.len();
        reserve_retained_slots(&mut self.dirty, needed, Some(work), &mut self.slots)
    }

    pub(super) fn admit_lazy_slots(
        &mut self,
        key: RecordKey,
        work: &crate::NativeMaterializationWork,
    ) -> Result<crate::NativeWorkReservation, LocalCoreStateStoreError> {
        let dirty = self.residents().admit_record(key, self.epoch, work)?;
        let needed = self.dirty.len().checked_add(usize::from(dirty)).ok_or(
            crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
        )?;
        reserve_retained_slots(&mut self.dirty, needed, Some(work), &mut self.slots)?;
        Ok(work.reserve_work(Residents::installation_work(1)?)?)
    }
}
