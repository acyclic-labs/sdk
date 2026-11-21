use super::*;

/// Only this fixed-size receipt changes. Intent and acknowledged preimages
/// are published once, before the receipt that makes them reachable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Receipt {
    version: u32,
    pub(super) history_id: OperationId,
    pub(super) revision: u64,
    root_identity: Option<[u8; 16]>,
    preparation_identity: Option<crate::MaterializationPreparationIdentity>,
    preparation_progress: Option<crate::MaterializationPreparationProgress>,
    pub(super) work: crate::WorkCounters,
    preimages: usize,
    pub(super) phase: crate::MaterializationPhase,
    applied: u32,
    restored: u32,
}

impl From<&MaterializationJournal> for Receipt {
    fn from(journal: &MaterializationJournal) -> Self {
        Self {
            version: journal.version,
            history_id: journal.history_id,
            revision: journal.revision,
            root_identity: journal.root_identity,
            preparation_identity: journal.preparation_identity,
            preparation_progress: journal.preparation_progress,
            work: journal.work,
            preimages: journal.preimages.len(),
            phase: journal.phase,
            applied: journal.applied,
            restored: journal.restored,
        }
    }
}

pub(super) const FORMAT: acyclic_log_frame::LogFrame = acyclic_log_frame::LogFrame {
    domain: b"materialization-log-v1\0",
    ..log::FRAME_FORMAT
};

/// A checksum-validated receipt for the same held file extent.
#[derive(Clone, Copy, Debug)]
pub(in crate::core_state) struct CachedCut {
    pub(super) operation: OperationId,
    identity: acyclic_native_runtime::FileIdentity,
    pub(super) end: u64,
    pub(super) live_data: u64,
    pub(super) current: Receipt,
    trailer: [u8; acyclic_log_frame::LogFrame::CHECKSUM_BYTES],
}

/// Complete transactions end in a receipt. A valid data-only suffix remains
/// unreachable until a receipt closes it.
pub(super) struct Log {
    pub(super) file: File,
    pub(super) end: u64,
    pub(super) live_data: u64,
    pub(super) current: Option<Receipt>,
    length: u64,
    pub(super) identity: acyclic_native_runtime::FileIdentity,
    bytes: Option<LogBuffer>,
}

impl Log {
    pub(super) fn open(
        paths: &RecordPaths<'_>,
        operation: OperationId,
        cached: Option<CachedCut>,
    ) -> Result<Option<Self>, LocalCoreStateStoreError> {
        let work = paths
            .allocation
            .as_ref()
            .map(|allocation| allocation.work());
        let file = match work {
            Some(work) => match materialization_open_options(
                &paths.current,
                &crate::native_host::NativeOpenOptions::ReadWrite,
                work,
            ) {
                Ok(file) => file,
                Err(NativeTreeMaterializationError::Io(error))
                    if error.kind() == std::io::ErrorKind::NotFound =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error.into()),
            },
            None => match OpenOptions::new()
                .read(true)
                .write(true)
                .open(&paths.current)
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            },
        };
        let identity = journal_identity(&file, work)?;
        let length = match work {
            Some(work) => crate::native_host::stat_file(&file, &mut &*work)?.len(),
            None => file.metadata()?.len(),
        };
        if let Some(cut) = cached.filter(|cut| {
            cut.operation == operation && cut.identity == identity && cut.end == length
        }) {
            if cut.matches_trailer(&file, work)? {
                return Ok(Some(Self {
                    file,
                    identity,
                    length,
                    end: length,
                    live_data: cut.live_data,
                    current: Some(cut.current),
                    bytes: None,
                }));
            }
        }
        let bytes = read_log_bytes(&file, work, None)?;
        let mut offset = 0;
        let mut end = 0;
        let mut current: Option<Receipt> = None;
        let mut plan = false;
        let mut data = 0;
        let mut live_data = 0;
        let mut images = 0;
        let mut start = 0;
        while let Some(frame) = checked_log_frame(FORMAT, &bytes.bytes, &mut offset, work)? {
            let (tag, payload) = split(frame)?;
            match tag {
                b'p' if !plan => {
                    plan = true;
                    data += (offset - start) as u64;
                }
                b'i' if plan => {
                    images += 1;
                    data += (offset - start) as u64;
                }
                b'r' if plan => {
                    let (receipt, _) = decode::<Receipt>(payload, work)?;
                    if receipt.version != crate::materializer::JOURNAL_VERSION
                        || receipt.revision == 0
                        || receipt.preimages != images
                        || current.is_some_and(|old| {
                            old.history_id != receipt.history_id || old.revision >= receipt.revision
                        })
                    {
                        return Err(LocalCoreStateStoreError::Integrity);
                    }
                    end = offset as u64;
                    live_data = data;
                    current = Some(receipt);
                }
                _ => return Err(LocalCoreStateStoreError::Integrity),
            }
            start = offset;
        }
        Ok(Some(Self {
            file,
            end,
            live_data,
            current,
            identity,
            length,
            bytes: Some(bytes),
        }))
    }

    pub(super) fn cut_tail(
        &self,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        if self.length != self.end {
            match work {
                Some(work) => crate::native_host::truncate_file(&self.file, self.end, &mut &*work)?,
                None => acyclic_native_runtime::truncate_file(&self.file, self.end)?,
            }
        }
        Ok(())
    }

    /// Copies the immutable frames covered by the last receipt to a new file.
    pub(super) fn copy_live_data(
        &self,
        target: &File,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        let bytes = self
            .bytes
            .as_ref()
            .ok_or(LocalCoreStateStoreError::Integrity)?;
        let bytes = bytes
            .bytes
            .get(..self.end as usize)
            .ok_or(LocalCoreStateStoreError::Integrity)?;
        let mut start = 0;
        let mut end = 0;
        let mut output = 0;
        while let Some(frame) = checked_log_frame(FORMAT, bytes, &mut end, work)? {
            let (tag, _) = split(frame)?;
            if tag != b'r' {
                let encoded = bytes
                    .get(start..end)
                    .ok_or(LocalCoreStateStoreError::Integrity)?;
                write_append_batch(output, encoded, work, |position, bytes| {
                    acyclic_native_runtime::WriteAt::new(target, position).write(bytes)
                })?;
                output += encoded.len() as u64;
            }
            start = end;
        }
        if output != self.live_data {
            return Err(LocalCoreStateStoreError::Integrity);
        }
        Ok(())
    }

    pub(super) fn load(
        &self,
        operation: OperationId,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Option<MaterializationJournal>, LocalCoreStateStoreError> {
        let Some(current) = self.current else {
            return Ok(None);
        };
        let mut offset = 0;
        let bytes = self
            .bytes
            .as_ref()
            .ok_or(LocalCoreStateStoreError::Integrity)?
            .bytes
            .get(..self.end as usize)
            .ok_or(LocalCoreStateStoreError::Integrity)?;
        let Some(frame) = checked_log_frame(FORMAT, bytes, &mut offset, work)? else {
            return Err(LocalCoreStateStoreError::Integrity);
        };
        let (b'p', payload) = split(frame)? else {
            return Err(LocalCoreStateStoreError::Integrity);
        };
        let (plan, mut custody) = decode::<crate::MaterializationPlan>(payload, work)?;
        if plan.operation_id != operation || current.preimages > plan.edits.len() {
            return Err(LocalCoreStateStoreError::Integrity);
        }
        let mut preimages = Vec::new();
        if let Some(custody) = &mut custody {
            crate::materializer::reserve_buffer(&mut preimages, current.preimages, custody)?;
        } else {
            preimages
                .try_reserve_exact(current.preimages)
                .map_err(|_| LocalCoreStateStoreError::Integrity)?;
        }
        while offset < bytes.len() {
            let Some(frame) = checked_log_frame(FORMAT, bytes, &mut offset, work)? else {
                return Err(LocalCoreStateStoreError::Integrity);
            };
            let (tag, payload) = split(frame)?;
            if tag == b'i' {
                let (image, retained) = decode::<crate::MaterializationPreimage>(payload, work)?;
                absorb(&mut custody, retained)?;
                preimages.push(image);
            }
        }
        let mut journal = MaterializationJournal::with_history(
            plan,
            current.root_identity,
            current.phase,
            current.history_id,
        );
        journal.revision = current.revision;
        journal.preparation_identity = current.preparation_identity;
        journal.preparation_progress = current.preparation_progress;
        journal.work = current.work;
        journal.preimages = preimages;
        journal.applied = current.applied;
        journal.restored = current.restored;
        if let Some(custody) = custody {
            journal.fund(custody)?;
        }
        Ok(Some(journal))
    }
}

pub(super) fn journal_identity(
    file: &File,
    work: Option<&crate::NativeMaterializationWork>,
) -> Result<acyclic_native_runtime::FileIdentity, LocalCoreStateStoreError> {
    let operation = crate::WorkCounters {
        backend_read_operations: 1,
        ..Default::default()
    };
    if let Some(work) = work {
        work.charge(operation)?;
    }
    Ok(acyclic_native_runtime::file_identity(file)?)
}

impl CachedCut {
    pub(super) fn new(
        operation: OperationId,
        identity: acyclic_native_runtime::FileIdentity,
        end: u64,
        live_data: u64,
        current: Receipt,
        encoded: &[u8],
    ) -> Result<Self, LocalCoreStateStoreError> {
        let trailer = *encoded
            .last_chunk()
            .ok_or(LocalCoreStateStoreError::Integrity)?;
        Ok(Self {
            operation,
            identity,
            end,
            live_data,
            current,
            trailer,
        })
    }

    fn matches_trailer(
        &self,
        file: &File,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<bool, LocalCoreStateStoreError> {
        let mut actual = [0; acyclic_log_frame::LogFrame::CHECKSUM_BYTES];
        let offset = self.end - actual.len() as u64;
        let mut read = || acyclic_native_runtime::read_at(file, offset, &mut actual);
        let count = match work {
            Some(work) => crate::native_host::file_transfer(
                acyclic_native_runtime::NativeOperationKind::Read,
                acyclic_log_frame::LogFrame::CHECKSUM_BYTES,
                work,
                read,
            )?,
            None => read()?,
        };
        Ok(count == actual.len() && actual == self.trailer)
    }
}

fn split(frame: &[u8]) -> Result<(u8, &[u8]), LocalCoreStateStoreError> {
    match frame {
        [b'[', b'"', tag, b'"', b',', body @ .., b']'] => Ok((*tag, body)),
        _ => Err(LocalCoreStateStoreError::Integrity),
    }
}

fn decode<T: DeserializeOwned>(
    bytes: &[u8],
    work: Option<&crate::NativeMaterializationWork>,
) -> Result<(T, Option<crate::NativeAllocation>), LocalCoreStateStoreError> {
    match work {
        Some(work) => measured_decode_reader(std::io::Cursor::new(bytes), bytes.len() as u64, work)
            .map(|(value, custody)| (value, Some(custody))),
        None => Ok((serde_json::from_slice(bytes)?, None)),
    }
}

pub(super) fn encode(
    current: Option<Receipt>,
    journal: &MaterializationJournal,
    work: Option<&crate::NativeMaterializationWork>,
) -> Result<(LogBuffer, u64), LocalCoreStateStoreError> {
    let previous = current.map_or(0, |record| record.preimages);
    if previous > journal.preimages.len() {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    let mut batch = LogBuffer {
        bytes: Vec::new(),
        allocation: work
            .map(|work| work.claim(0, 0, crate::WorkCounters::UNCHARGED))
            .transpose()?,
    };
    if current.is_none() {
        batch.push_frame(FORMAT, &("p", &journal.plan))?;
    }
    for image in journal.preimages.iter().skip(previous) {
        batch.push_frame(FORMAT, &("i", image))?;
    }
    let new_data = batch.bytes.len() as u64;
    batch.push_frame(FORMAT, &("r", Receipt::from(journal)))?;
    Ok((batch, new_data))
}
fn absorb(
    retained: &mut Option<crate::NativeAllocation>,
    additional: Option<crate::NativeAllocation>,
) -> Result<(), LocalCoreStateStoreError> {
    if let Some(mut additional) = additional {
        match retained {
            Some(retained) => retained.absorb(&mut additional)?,
            None => *retained = Some(additional),
        }
    }
    Ok(())
}
