//! Atomic private checkpoints retain logical state and retry outcomes, not history.
use super::*;

const PACK_LIMIT: usize = 64 * 1024;
const TEMPORARY: &str = ".v1-checkpoint.tmp";

impl Journal {
    pub(super) fn compact(
        &self,
        state: &State,
        meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> Result<u64, NativeObjectsError> {
        obs::scoped(
            &obs::span!(
                DEBUG,
                "acyclic.objects.journal.checkpoint",
                frames = obs::Empty,
                bytes = obs::Empty,
                reclaimed = obs::Empty
            ),
            || {
                // Temporary storage drops before publication custody.
                let mut publication = Publication::new(meter);
                publication.check()?;
                self.check()?;
                let snapshot = difference(&State::default(), state, 0, &mut publication)?;
                let temporary = crate::physical::local_path(&self.root.path, TEMPORARY, publication.meter.as_ref())?;
                let destination =
                    crate::physical::local_path(&self.root.path, "mutations.log", publication.meter.as_ref())?;
                let mut file = crate::physical::open_publication(&temporary.path, &publication)?;
                let header = publication.encode(&self.header)?;
                let header_length = u32::try_from(header.len())
                    .map_err(|_| Error::from(QuotaExceeded))?;
                let digest = publication.codec(0, 0, header.len(), || blake3::hash(&header))?;
                crate::physical::write_publication(&mut file, 0, MAGIC, &mut publication)?;
                crate::physical::write_publication(
                    &mut file, MAGIC.len() as u64, &header_length.to_le_bytes(), &mut publication,
                )?;
                crate::physical::write_publication(
                    &mut file, (MAGIC.len() + 4) as u64, digest.as_bytes(), &mut publication,
                )?;
                crate::physical::write_publication(
                    &mut file, (MAGIC.len() + 36) as u64, &header, &mut publication,
                )?;
                let mut output = Checkpoint {
                    file,
                    limits: self.limits,
                    operations: 0,
                    bytes: (MAGIC.len() + 36 + header.len()) as u64,
                    pending: Delta {
                        sequence: state.sequence,
                        ..Default::default()
                    },
                };
                output.snapshot(snapshot, &mut publication)?;
                #[cfg(test)]
                self.checkpoint_fault(1).map_err(|_| Error::from(Unavailable))?;
                crate::physical::sync_publication(
                    &output.file, self.limits.durability, &publication,
                )?;
                #[cfg(test)]
                self.checkpoint_fault(2).map_err(|_| Error::from(Unavailable))?;
                let mut tail = self.tail.lock().map_err(|_| Error::from(Unavailable))?;
                let reclaimed = tail.bytes.saturating_sub(output.bytes);
                // Close the old journal before Windows replacement.
                drop(std::mem::replace(&mut tail.file, output.file));
                #[cfg(test)]
                self.checkpoint_fault(3).map_err(|_| Error::from(Unavailable))?;
                if let Err(error) = self.root.replace_journal(|| {
                    match &publication.meter {
                        Some(meter) => acyclic_native_runtime::durable_rename(
                            &temporary.path, &destination.path,
                            acyclic_native_runtime::RenameMode::Replace, Some(meter),
                        ),
                        None => acyclic_native_runtime::durable_rename(
                            &temporary.path, &destination.path,
                            acyclic_native_runtime::RenameMode::Replace, None,
                        ),
                    }
                }) {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(NativeObjectsError::Admission(error));
                }
                #[cfg(test)]
                self.checkpoint_fault(4).map_err(|_| Error::from(Unavailable))?;
                tail.bytes = output.bytes;
                tail.operations = output.operations;
                tail.inline_bytes = 0;
                obs::record!(
                    "frames" = output.operations,
                    "bytes" = output.bytes,
                    "reclaimed" = reclaimed,
                );
                Ok(reclaimed)
            },
        )
    }

    #[cfg(test)]
    fn checkpoint_fault(&self, stage: u64) -> Result<(), LocalOpenError> {
        if self.fault_checkpoint.load(Ordering::Acquire) == stage {
            self.poisoned.store(true, Ordering::Release);
            return Err(LocalOpenError::Unavailable);
        }
        Ok(())
    }
}

struct Checkpoint {
    file: File,
    limits: LocalObjectsLimits,
    operations: u64,
    bytes: u64,
    pending: Delta,
}
impl Checkpoint {
    fn snapshot(
        &mut self,
        snapshot: Delta,
        publication: &mut Publication,
    ) -> Result<(), NativeObjectsError> {
        // Pack buckets, objects, uploads/parts, then receipts.
        macro_rules! append {
            ($field:ident) => {
                for item in snapshot.$field {
                    if self.has_changes()
                        && self
                            .pending
                            .encoded_len()
                            .saturating_add(item.encoded_len() + 12)
                            > PACK_LIMIT
                    {
                        self.flush(publication)?;
                    }
                    publication.push(&mut self.pending.$field, item)?;
                }
            };
        }
        append!(buckets);
        append!(objects);
        append!(uploads);
        append!(parts);
        append!(receipts);
        if self.has_changes() || self.operations == 0 && self.pending.sequence != 0 {
            self.flush(publication)?;
        }
        Ok(())
    }
    fn has_changes(&self) -> bool {
        !self.pending.buckets.is_empty()
            || !self.pending.objects.is_empty()
            || !self.pending.uploads.is_empty()
            || !self.pending.parts.is_empty()
            || !self.pending.receipts.is_empty()
    }
    fn flush(&mut self, publication: &mut Publication) -> Result<(), NativeObjectsError> {
        // Multipart frames must fit the authenticated record limit.
        self.pending.ordinal = self.operations.checked_add(1)
            .ok_or(Error::from(QuotaExceeded))?;
        let length = self.pending.encoded_len();
        let end = self.bytes.checked_add(36 + length as u64)
            .ok_or(Error::from(QuotaExceeded))?;
        if length > RECORD_LIMIT
            || end > self.limits.maximum_journal_bytes
            || self.pending.ordinal > self.limits.maximum_journal_operations
        {
            return Err(Error::from(QuotaExceeded).into());
        }
        // Each encoded frame owns temporary custody.
        let mut frame = Publication::new(publication.meter.clone());
        let encoded = frame.encode(&self.pending)?;
        let length = u32::try_from(encoded.len()).map_err(|_| Error::from(QuotaExceeded))?;
        let digest = frame.codec(0, 0, encoded.len(), || blake3::hash(&encoded))?;
        crate::physical::write_publication(
            &mut self.file, self.bytes, &length.to_le_bytes(), &mut frame,
        )?;
        crate::physical::write_publication(
            &mut self.file, self.bytes + 4, digest.as_bytes(), &mut frame,
        )?;
        crate::physical::write_publication(
            &mut self.file, self.bytes + 36, &encoded, &mut frame,
        )?;
        self.bytes = end;
        self.operations = self.pending.ordinal;
        // Reuse pending-array capacities without changing packing inputs.
        self.pending.ordinal = 0;
        self.pending.buckets.clear();
        self.pending.objects.clear();
        self.pending.uploads.clear();
        self.pending.parts.clear();
        self.pending.receipts.clear();
        self.pending.inline_data.clear();
        Ok(())
    }
}
