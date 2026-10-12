use super::*;

impl CoreLog {
    pub(super) fn adopt_native(&mut self, work: &crate::materializer::NativeMaterializationWork) -> Result<(), LocalCoreStateStoreError> {
        self.residents().adopt_native(work)?;
        let needed = self.dirty.len();
        reserve_slots(&mut self.dirty, needed, work, &mut self.slots)
    }

    pub(super) fn get_native_value(
        &mut self,
        root: &Namespace,
        key: RecordKey,
        work: &crate::materializer::NativeMaterializationWork,
        cxl: &crate::CancellationToken,
    ) -> Result<Option<NativeOwnedCoreState<RecordValue>>, LocalCoreStateStoreError> {
        if cxl.is_cancelled() {
            return Err(crate::materializer::NativeTreeMaterializationError::Cancelled.into());
        }
        self.adopt_native(work)?;
        {
            let residents = self.residents();
            for record in &residents.0 {
                work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                if record.key == key {
                    return record.value.as_ref().map(|value| native_records::clone_native_record_value(value, work, cxl)).transpose();
                }
            }
        }
        let (paths, _paths_allocation) = key.measured_paths(root, work)?;
        let value = native_records::read_native_record(key, &paths.current, work, cxl)?;
        let output = value.as_ref().map(|value| native_records::clone_native_record_value(value.as_state(), work, cxl)).transpose()?;
        let mut residents = self.residents();
        let needed = residents.0.len() + 1;
        let Residents(values, slots) = &mut *residents;
        reserve_slots(values, needed, work, slots)?;
        let (value, allocation) = match value {
            Some(value) => { let (value, allocation) = value.into_parts(); (Some(value), allocation) },
            None => (None, work.claim(0, 0, crate::WorkCounters::UNCHARGED)?),
        };
        residents.0.push(Resident { key, value, allocation: Some(allocation) });
        Ok(output)
    }
    pub(super) fn admit_lazy_slots(
        &mut self,
        key: RecordKey,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(Option<usize>, bool), LocalCoreStateStoreError> {
        let mut residents = self.residents();
        let mut found = None;
        for (index, record) in residents.0.iter().enumerate() {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            if record.key == key { found = Some(index); break; }
        }
        if found.is_none() {
            let needed = residents.0.len() + 1;
            let Residents(values, slots) = &mut *residents;
            reserve_slots(values, needed, work, slots)?;
        }
        drop(residents);
        let mut dirty = false;
        for candidate in &self.dirty {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            if *candidate == key { dirty = true; break; }
        }
        if !dirty {
            let needed = self.dirty.len() + 1;
            reserve_slots(&mut self.dirty, needed, work, &mut self.slots)?;
        }
        Ok((found, !dirty))
    }

    pub(super) fn get_lazy_measured(
        &mut self,
        root: &Namespace,
        id: WorkspaceId,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<LazyWorkspaceState>, LocalCoreStateStoreError> {
        self.get_native_value(root, RecordKey::LazyWorkspace(id), work, work.cancellation())?
            .map(|owned| {
                let (value, allocation) = owned.into_parts();
                let _allocation = allocation;
                match value {
                    RecordValue::LazyWorkspace(value) => Ok(value),
                    _ => Err(LocalCoreStateStoreError::Integrity),
                }
            }).transpose()
    }

}
