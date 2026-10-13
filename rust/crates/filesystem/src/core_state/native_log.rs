use super::*;

impl CoreLog {
    pub(super) fn adopt_native(
        &mut self,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.residents().adopt_native(work)?;
        let needed = self.dirty.len();
        reserve_slots(&mut self.dirty, needed, work, &mut self.slots)
    }

    pub(super) fn admit_lazy_slots(
        &mut self,
        key: RecordKey,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(Option<usize>, bool), LocalCoreStateStoreError> {
        let mut residents = self.residents();
        let found = residents.position(key, Some(work))?;
        if found.is_none() {
            let needed = residents.0.len() + 1;
            let Residents(values, slots) = &mut *residents;
            reserve_slots(values, needed, work, slots)?;
        }
        drop(residents);
        let mut dirty = false;
        for candidate in &self.dirty {
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            if *candidate == key {
                dirty = true;
                break;
            }
        }
        if !dirty {
            let needed = self.dirty.len() + 1;
            reserve_slots(&mut self.dirty, needed, work, &mut self.slots)?;
        }
        Ok((found, !dirty))
    }
}
