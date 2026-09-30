//! Atomic private checkpoints retain logical state and retry outcomes, not history.
use super::*;

const PACK_LIMIT: usize = 64 * 1024;
const TEMPORARY: &str = ".v2-checkpoint.tmp";

impl Journal {
    pub(super) fn compact(&self, state: &State) -> Result<u64, LocalOpenError> {
        self.check().map_err(|_| LocalOpenError::Unavailable)?;
        let snapshot =
            difference(&State::default(), state, 0).map_err(|_| LocalOpenError::Corrupt)?;
        let temporary = self.root.join(TEMPORARY);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&temporary)?;
        let header = self.header.encode_to_vec();
        file.write_all(MAGIC)?;
        file.write_all(&u32::try_from(header.len()).map_err(corrupt)?.to_le_bytes())?;
        file.write_all(blake3::hash(&header).as_bytes())?;
        file.write_all(&header)?;
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
        output.snapshot(snapshot)?;
        #[cfg(test)]
        self.checkpoint_fault(1)?;
        sync(&output.file, self.limits)?;
        #[cfg(test)]
        self.checkpoint_fault(2)?;
        let mut tail = self.tail.lock().map_err(|_| LocalOpenError::Unavailable)?;
        let reclaimed = tail.bytes.saturating_sub(output.bytes);
        // Closing the old journal permits Windows replacement. The replacement
        // handle retains the fully synchronized checkpoint across the rename.
        drop(std::mem::replace(&mut tail.file, output.file));
        #[cfg(test)]
        self.checkpoint_fault(3)?;
        if let Err(error) = acyclic_native_runtime::durable_rename(
            &temporary,
            &self.root.join("mutations.log"),
            acyclic_native_runtime::RenameMode::Replace,
        ) {
            self.poisoned.store(true, Ordering::Release);
            return Err(error.into());
        }
        #[cfg(test)]
        self.checkpoint_fault(4)?;
        tail.bytes = output.bytes;
        tail.operations = output.operations;
        Ok(reclaimed)
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
    fn snapshot(&mut self, snapshot: Delta) -> Result<(), LocalOpenError> {
        // Each frame admits only states within the final live byte capacity:
        // create buckets, current objects, uploads/parts, then exact receipts.
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
                        self.flush()?;
                    }
                    self.pending.$field.push(item);
                }
            };
        }
        append!(buckets);
        append!(objects);
        append!(uploads);
        append!(parts);
        append!(receipts);
        if self.has_changes() || self.operations == 0 && self.pending.sequence != 0 {
            self.flush()?;
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
    fn flush(&mut self) -> Result<(), LocalOpenError> {
        // A single multipart object may exceed the packing target while still
        // fitting the journal's existing authenticated record limit.
        self.pending.ordinal = self
            .operations
            .checked_add(1)
            .ok_or(LocalOpenError::Invalid)?;
        let encoded = self.pending.encode_to_vec();
        let end = self
            .bytes
            .checked_add(36 + encoded.len() as u64)
            .ok_or(LocalOpenError::Invalid)?;
        if encoded.len() > RECORD_LIMIT
            || end > self.limits.maximum_journal_bytes
            || self.pending.ordinal > self.limits.maximum_journal_operations
        {
            return Err(LocalOpenError::Invalid);
        }
        self.file
            .write_all(&u32::try_from(encoded.len()).map_err(corrupt)?.to_le_bytes())?;
        self.file.write_all(blake3::hash(&encoded).as_bytes())?;
        self.file.write_all(&encoded)?;
        self.bytes = end;
        self.operations = self.pending.ordinal;
        self.pending = Delta {
            sequence: self.pending.sequence,
            ..Default::default()
        };
        Ok(())
    }
}
