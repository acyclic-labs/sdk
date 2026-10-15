use super::*;

impl MemoryObjects {
    pub(in crate::v1) fn local_maintenance_due(&self) -> Result<bool, Error> {
        let _state = self.lock_state()?;
        let journal = self.journal.as_ref().ok_or(Error::from(Unavailable))?;
        journal.maintenance_due()
    }
    pub(in crate::v1) fn compact_local_if_due(
        &self,
        meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> Result<(), NativeObjectsError> {
        obs::scoped(&obs::span!(DEBUG, "acyclic.objects.compact"), || {
            if let Some(meter) = &meter {
                meter.check()?;
            }
            let mut state = self.lock_state()?;
            let journal = self.journal.as_ref().ok_or(Error::from(Unavailable))?;
            if !journal.maintenance_due()? {
                return Ok(());
            }
            if let Some(meter) = &meter {
                meter.check()?;
            }
            let mut next = state.clone();
            journal.materialize_inline(&mut next, meter.clone())?;
            journal.compact(&next, meter)?;
            *state = next;
            Ok(())
        })
    }
    pub(in crate::v1) fn collect_local_garbage(
        &self,
        maximum_candidates: u64,
    ) -> Result<crate::LocalObjectsGarbageCollection, LocalOpenError> {
        obs::scoped(
            &obs::span!(
                INFO,
                "acyclic.objects.collect_garbage",
                segments = obs::Empty,
                reclaimed = obs::Empty
            ),
            || {
                let mut state = self.lock_state().map_err(|_| LocalOpenError::Unavailable)?;
                let journal = self.journal.as_ref().ok_or(LocalOpenError::Unavailable)?;
                let mut next = state.clone();
                journal
                    .materialize_inline(&mut next, None)
                    .map_err(open_error)?;
                let mut references = BTreeSet::new();
                for bucket in next.buckets.values() {
                    for object in bucket.objects.values() {
                        object.body.local_references(&mut references);
                    }
                }
                for upload in next.uploads.values() {
                    for (_, body) in upload.parts.values() {
                        body.local_references(&mut references);
                    }
                }
                let mut report = crate::physical::collect_physical_garbage(
                    &journal.root.path,
                    &references,
                    maximum_candidates,
                    journal.limits.maximum_object_bytes,
                    journal.limits.durability,
                )
                .map_err(|error| match error {
                    crate::physical::PhysicalError::Invalid(_) => LocalOpenError::Invalid,
                    crate::physical::PhysicalError::Io(error) => LocalOpenError::Io(error),
                    crate::physical::PhysicalError::Corrupt => LocalOpenError::Corrupt,
                })?;
                report.journal_bytes_reclaimed = journal.compact(&next, None).map_err(|error| match error {
                    error if !error.has_payload() && matches!(&error.cause, NativeObjectsCause::Service(error) if error.code == QuotaExceeded) => LocalOpenError::Invalid,
                    error => open_error(error),
                })?;
                *state = next;
                obs::record!(
                    "segments" = report.segments_removed,
                    "reclaimed" = report.journal_bytes_reclaimed,
                );
                Ok(report)
            },
        )
    }
}
