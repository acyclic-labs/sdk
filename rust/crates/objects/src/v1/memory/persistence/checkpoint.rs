//! Atomic private checkpoints retain logical state and retry outcomes, not history.
use super::*;

const PACK_LIMIT: usize = 64 * 1024;
const TEMPORARY: &str = ".v1-checkpoint.tmp";

mod packing;
mod snapshot;
#[cfg(test)]
mod tests;
use packing::Pending;
use snapshot::Change;

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
                let temporary = crate::physical::local_path(
                    &self.root.path,
                    TEMPORARY,
                    publication.native.as_ref().map(|(meter, _)| meter),
                )?;
                let destination = crate::physical::local_path(
                    &self.root.path,
                    "mutations.log",
                    publication.native.as_ref().map(|(meter, _)| meter),
                )?;
                let file = crate::physical::open_publication(&temporary.value, &publication)?;
                let mut output = Checkpoint::begin(file, self, state.sequence, &mut publication)?;
                output.snapshot(state)?;
                #[cfg(test)]
                self.checkpoint_fault(1)?;
                crate::physical::sync_publication(
                    &output.file,
                    self.limits.durability,
                    &publication,
                )?;
                #[cfg(test)]
                self.checkpoint_fault(2)?;
                let journal = journal_file(output.file, &destination.value, publication.meter())?;
                let mut tail = self.tail.lock().map_err(|_| Error::from(Unavailable))?;
                let reclaimed = tail.bytes.saturating_sub(output.bytes);
                // Close the old journal before Windows replacement.
                drop(std::mem::replace(&mut tail.file, journal));
                #[cfg(test)]
                self.checkpoint_fault(3)?;
                if let Err(error) = self.root.replace_journal(|| {
                    acyclic_native_runtime::durable_rename(
                        &temporary.value,
                        &destination.value,
                        acyclic_native_runtime::RenameMode::Replace,
                        publication.native.as_ref().map(|(meter, _)| meter),
                    )
                }) {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(error.into());
                }
                #[cfg(test)]
                self.checkpoint_fault(4)?;
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
    fn checkpoint_fault(&self, stage: u64) -> Result<(), Error> {
        if self.fault_checkpoint.load(Ordering::Acquire) == stage {
            self.poisoned.store(true, Ordering::Release);
            return Err(Error::from(Unavailable));
        }
        Ok(())
    }
}

struct Checkpoint {
    file: File,
    limits: LocalObjectsLimits,
    operations: u64,
    bytes: u64,
    pending: Pending,
}
impl Checkpoint {
    fn begin(
        file: File,
        journal: &Journal,
        sequence: u64,
        publication: &mut Publication,
    ) -> Result<Self, NativeObjectsError> {
        let header = publication.encode(&journal.header)?;
        let length = u32::try_from(header.len()).map_err(|_| Error::from(QuotaExceeded))?;
        let digest = publication.codec(0, 0, header.len(), || blake3::hash(&header))?;
        crate::physical::write_publication(&file, 0, MAGIC, publication)?;
        crate::physical::write_publication(
            &file,
            MAGIC.len() as u64,
            &length.to_le_bytes(),
            publication,
        )?;
        crate::physical::write_publication(
            &file,
            (MAGIC.len() + 4) as u64,
            digest.as_bytes(),
            publication,
        )?;
        crate::physical::write_publication(&file, (MAGIC.len() + 36) as u64, &header, publication)?;
        Ok(Self {
            file,
            limits: journal.limits,
            operations: 0,
            bytes: (MAGIC.len() + 36 + header.len()) as u64,
            pending: Pending::new(
                sequence,
                publication
                    .native
                    .as_ref()
                    .map(|(meter, _)| Arc::clone(meter)),
            ),
        })
    }

    fn flush(&mut self) -> Result<(), NativeObjectsError> {
        let replacement = Pending::new(self.pending.delta.sequence, self.pending.meter());
        // Values and their original ledger move together and retire together.
        let mut pending = std::mem::replace(&mut self.pending, replacement);
        // Multipart frames must fit the authenticated record limit.
        pending.delta.ordinal = self
            .operations
            .checked_add(1)
            .ok_or(Error::from(QuotaExceeded))?;
        let length = pending.delta.encoded_len();
        let end = self
            .bytes
            .checked_add(36 + length as u64)
            .ok_or(Error::from(QuotaExceeded))?;
        if length > RECORD_LIMIT
            || end > self.limits.maximum_journal_bytes
            || pending.delta.ordinal > self.limits.maximum_journal_operations
        {
            return Err(Error::from(QuotaExceeded).into());
        }
        // Each encoded frame owns temporary custody.
        let frame = &mut pending.publication;
        let encoded = frame.encode(&pending.delta)?;
        let length = u32::try_from(encoded.len()).map_err(|_| Error::from(QuotaExceeded))?;
        let digest = frame.codec(0, 0, encoded.len(), || blake3::hash(&encoded))?;
        crate::physical::write_publication(&self.file, self.bytes, &length.to_le_bytes(), frame)?;
        crate::physical::write_publication(&self.file, self.bytes + 4, digest.as_bytes(), frame)?;
        crate::physical::write_publication(&self.file, self.bytes + 36, &encoded, frame)?;
        self.bytes = end;
        self.operations = pending.delta.ordinal;
        Ok(())
    }
}
