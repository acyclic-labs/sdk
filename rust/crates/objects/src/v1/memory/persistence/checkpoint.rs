//! Atomic private checkpoints retain logical state and retry outcomes, not history.
use super::*;

const PACK_LIMIT: usize = 64 * 1024;
const TEMPORARY: &str = ".v1-checkpoint.tmp";

impl Journal {
    pub(super) fn compact(&self, state: &State) -> Result<u64, LocalOpenError> {
        self.compact_with(state, None).map_err(|error| match error {
            NativeObjectsError::Service(error) if error.code == QuotaExceeded => {
                LocalOpenError::Invalid
            }
            error => open_error(error),
        })
    }

    /// Ordinary and current-original-meter checkpoints share the same frame, packing, synchronization, replacement and poison boundaries.
    pub(super) fn compact_with(
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
                // All snapshot, encoded-frame and pending-frame storage drops
                // before this temporary publication's allocation custody.
                let mut publication = Publication::new(meter);
                publication.check()?;
                self.check()?;
                let snapshot = difference_with(&State::default(), state, 0, &mut publication)?;
                let temporary = CheckpointPath::new(&self.root.path, TEMPORARY, &publication)?;
                let destination =
                    CheckpointPath::new(&self.root.path, "mutations.log", &publication)?;
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
                // Closing the old journal permits Windows replacement. The replacement
                // handle retains the fully synchronized checkpoint across the rename.
                drop(std::mem::replace(&mut tail.file, output.file));
                #[cfg(test)]
                self.checkpoint_fault(3).map_err(|_| Error::from(Unavailable))?;
                if let Err(error) = self.root.replace_journal(|| {
                    match &publication.meter {
                        Some(meter) => acyclic_native_runtime::durable_rename_measured(
                            &temporary.path, &destination.path,
                            acyclic_native_runtime::RenameMode::Replace, meter,
                        ),
                        None => acyclic_native_runtime::durable_rename(
                            &temporary.path, &destination.path,
                            acyclic_native_runtime::RenameMode::Replace,
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

struct CheckpointPath {
    path: PathBuf,
    _allocation: Option<ReadAllocation>,
}
impl CheckpointPath {
    fn new(
        root: &Path,
        suffix: &str,
        publication: &Publication,
    ) -> Result<Self, NativeObjectsError> {
        let maximum = root.as_os_str().len().checked_add(suffix.len())
            .and_then(|length| length.checked_add(1))
            .ok_or(Error::from(QuotaExceeded))?;
        let mut allocation = publication.meter.as_ref()
            .map(|meter| ReadAllocation::new(meter, maximum)).transpose()?;
        let mut path = PathBuf::new();
        path.try_reserve_exact(maximum).map_err(|error| {
            publication.meter.as_ref().map_or_else(
                || NativeObjectsError::Service(Unavailable.into()),
                |meter| NativeObjectsError::Admission(meter.allocation_failed(error)),
            )
        })?;
        let reconciled = allocation.as_mut()
            .map(|allocation| allocation.reconcile_capacity(path.capacity())).transpose();
        let mut output = Self { path, _allocation: allocation };
        reconciled?;
        if let Some(meter) = &publication.meter {
            meter.admit_codec(0, maximum as u64, 0)?;
        }
        output.path.push(root);
        output.path.push(suffix);
        if let Some(meter) = &publication.meter {
            meter.record_codec(0, output.path.as_os_str().len() as u64, 0)?;
        }
        Ok(output)
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
        // A single multipart object may exceed the packing target while still
        // fitting the journal's existing authenticated record limit.
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
        // A frame's encoded buffer is temporary, not live checkpoint state.
        // Retire its claim when that buffer drops instead of accumulating all
        // previous encodings while the remaining snapshot is packed.
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
        // Preserve the packing inputs (including an unset ordinal) while
        // reusing the actual pending-array allocations between frames.
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
