use super::*;

#[derive(Debug)]
pub(super) struct CoreLog {
    pub(super) file: File,
    /// Bytes appended to the log.
    pub(super) length: u64,
    /// Bytes of the log known to be durable.
    pub(super) flushed: u64,
    pub(super) epoch: Uuid,
    pub(super) sequence: u64,
    pub(super) residents: Arc<RwLock<Residents>>,
    pub(super) dirty: Vec<RecordKey>,
    /// The log may hold a frame whose outcome is unknown, so it must be
    pub(super) broken: bool,
    pub(super) slots: Option<crate::materializer::NativeAllocation>,
    pub(super) residents_control: Option<crate::materializer::NativeAllocation>,
}

impl CoreLog {
    pub(super) fn open(
        root: &Namespace,
        residents: Arc<RwLock<Residents>>,
    ) -> Result<Self, LocalCoreStateStoreError> {
        let work = root
            ._allocation
            .as_ref()
            .map(|allocation| allocation.work());
        if let Some(work) = work {
            residents
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .adopt_native(work)?;
        }
        let paths = root.record(CORE_LOG_FAMILY, &[0; 16])?;
        let mut file = match work {
            Some(work) => {
                crate::native_host::create_directories(&paths.directory, &mut &*work)?;
                match materialization_open_options(
                    &paths.current,
                    &crate::native_host::NativeOpenOptions::ReadWriteCreateNew,
                    work,
                ) {
                    Ok(file) => {
                        materialization_sync(&paths.directory, work)?;
                        file
                    }
                    Err(crate::materializer::NativeTreeMaterializationError::Io(error))
                        if error.kind() == std::io::ErrorKind::AlreadyExists =>
                    {
                        materialization_open_options(
                            &paths.current,
                            &crate::native_host::NativeOpenOptions::ReadWrite,
                            work,
                        )?
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            None => {
                std::fs::create_dir_all(&paths.directory)?;
                match OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&paths.current)
                {
                    Ok(file) => {
                        sync_directory(&paths.directory)?;
                        file
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(&paths.current)?
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        };
        let mut bytes_allocation = work
            .map(|work| work.claim(0, 0, crate::WorkCounters::UNCHARGED))
            .transpose()?;
        let mut bytes = Vec::new();
        if let Some(work) = work {
            let length = crate::native_host::stat_file_with_work(&file, &mut &*work)?.len();
            let size = usize::try_from(length).map_err(|_| {
                crate::materializer::NativeTreeMaterializationError::Work(
                    crate::WorkError::Overflow,
                )
            })?;
            reserve(
                &mut bytes,
                size,
                work,
                bytes_allocation.as_mut().expect("read buffer"),
            )?;
            work.charge(crate::WorkCounters {
                bytes_copied: length,
                ..Default::default()
            })?;
            bytes.resize(size, 0);
            let mut position = 0;
            while position < bytes.len() {
                let remaining = work
                    .remaining()?
                    .source_bytes_read
                    .min(work.remaining()?.bytes_copied);
                let end = bytes
                    .len()
                    .min(position.saturating_add(64 * 1024))
                    .min(position.saturating_add(usize::try_from(remaining).unwrap_or(usize::MAX)));
                if end == position {
                    work.charge(crate::WorkCounters {
                        source_bytes_read: 1,
                        bytes_copied: 1,
                        ..Default::default()
                    })?;
                }
                let count =
                    crate::native_host::read_builtin_file(&file, &mut bytes[position..end], work)?;
                if count == 0 {
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
                }
                position += count;
            }
        } else {
            file.read_to_end(&mut bytes)?;
        }
        {
            let mut previous = residents.write().unwrap_or_else(PoisonError::into_inner);
            if let Some(work) = work {
                for record in &previous.0 {
                    let children = match &record.value {
                        Some(RecordValue::Context(context)) => context.roots.len(),
                        Some(RecordValue::Children(children)) => children.0.len(),
                        Some(RecordValue::FlatContext(context)) => context.roots.0.len(),
                        Some(RecordValue::FlatChildren(children)) => children.len(),
                        _ => 0,
                    };
                    work.charge(crate::WorkCounters {
                        items_examined: (children as u64).saturating_add(1),
                        ..Default::default()
                    })?;
                }
            }
            previous.clear();
        }
        let published =
            matches!(&root.admission, Admission::Owned(_)).then(|| Arc::clone(&residents));
        let _provisional_allocation = if published.is_some() {
            work.map(|work| {
                work.claim(
                    std::mem::size_of::<RwLock<Residents>>() as u64,
                    1,
                    crate::WorkCounters::UNCHARGED,
                )
            })
            .transpose()?
        } else {
            None
        };
        let replay_residents = if published.is_some() {
            Arc::default()
        } else {
            residents
        };
        let length = bytes.len() as u64;
        let mut log = Self {
            file,
            length,
            flushed: 0,
            epoch: Uuid::now_v7(),
            sequence: 0,
            residents: replay_residents,
            dirty: Vec::new(),
            broken: false,
            slots: None,
            residents_control: None,
        };
        let mut epoch = None;
        let mut sequence = 0;
        let mut line_start = 0;
        while line_start < bytes.len() {
            let mut line_end = line_start;
            while line_end < bytes.len() {
                if let Some(work) = work {
                    work.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    })?;
                }
                let byte = bytes[line_end];
                line_end += 1;
                if byte == b'\n' {
                    break;
                }
            }
            let line = &bytes[line_start..line_end];
            line_start = line_end;
            if let Some(work) = work {
                work.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            let Some(line) = line.strip_suffix(b"\n") else {
                break;
            };
            let Some((checksum, frame)) = line.split_first_chunk::<64>() else {
                break;
            };
            let Some(frame) = frame.strip_prefix(b" ") else {
                break;
            };
            if let Some(work) = work {
                work.charge(crate::WorkCounters {
                    bytes_hashed: frame.len() as u64,
                    ..Default::default()
                })?;
            }
            if blake3::hash(frame).to_hex().as_bytes() != checksum {
                break;
            }
            let Some(work) = work else {
                let mut frame: LogFrame<Change> = serde_json::from_slice(frame)
                    .map_err(|_| LocalCoreStateStoreError::Corrupt(paths.current.clone()))?;
                if *epoch.get_or_insert(frame.epoch) != frame.epoch || frame.sequence != sequence {
                    break;
                }
                for record in &mut frame.change.0 {
                    if let Some(value) = &mut record.value {
                        value.normalize(None)?;
                    }
                }
                log.apply(frame.change);
                sequence += 1;
                continue;
            };
            let (frame, mut frame_allocation) = match measured_decode_slice::<
                LogFrame<Vec<(RecordKey, Option<RecordValue>)>>,
            >(frame, work)
            {
                Ok(frame) => frame,
                Err(error @ LocalCoreStateStoreError::MaterializationWork(_)) => return Err(error),
                Err(_) => return Err(native_corrupt_log(paths)?),
            };
            // The decoded frame's physical allocations precede their guard's drop.
            let frame = frame;
            for (key, value) in &frame.change {
                work.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
                if value.as_ref().is_some_and(|value| !value.matches(*key)) {
                    return Err(native_corrupt_log(paths)?);
                }
            }
            if *epoch.get_or_insert(frame.epoch) != frame.epoch || frame.sequence != sequence {
                break;
            }
            for (key, mut value) in frame.change {
                let prior_owned = value
                    .as_ref()
                    .map(|value| native_records::native_record_owned_bytes(value, work))
                    .transpose()?
                    .unwrap_or(0);
                if let Some(value) = &mut value {
                    value.normalize(Some(work))?;
                }
                let normalized_owned = value
                    .as_ref()
                    .map(|value| native_records::native_record_owned_bytes(value, work))
                    .transpose()?
                    .unwrap_or(0);
                frame_allocation.release(prior_owned - normalized_owned)?;
                let (index, dirty) = log.admit_lazy_slots(key, work)?;
                let mut residents = log.residents();
                let record = Resident {
                    key,
                    value,
                    allocation: Some(frame_allocation.split_off(normalized_owned)?),
                };
                if let Some(index) = index {
                    residents.0[index] = record;
                } else {
                    residents.0.push(record);
                }
                drop(residents);
                if dirty {
                    log.dirty.push(key);
                }
            }
            drop(frame_allocation);
            sequence += 1;
        }
        if length != 0 {
            log.checkpoint(root)?;
        }
        if let Some(published) = published {
            {
                let mut destination = published.write().unwrap_or_else(PoisonError::into_inner);
                let mut replayed = log.residents();
                *destination = std::mem::take(&mut *replayed);
            }
            log.residents = published;
        }
        Ok(log)
    }

    pub(super) fn residents(&self) -> RwLockWriteGuard<'_, Residents> {
        self.residents
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Reads one record from memory, or from its file on first use.
    pub(super) fn get<T: Logged>(
        &mut self,
        root: &Namespace,
        id: T::Id,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        self.get_bounded(root, id, None)
    }

    /// Reads one record, refusing a file larger than `maximum_bytes`.
    pub(super) fn get_bounded<T: Logged>(
        &mut self,
        root: &Namespace,
        id: T::Id,
        maximum_bytes: Option<u64>,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        let key = T::key(id);
        if let Some(value) = self.residents().get(&key) {
            return match value {
                Some(RecordValue::FlatContext(_)) | Some(RecordValue::FlatChildren(_)) => Ok(
                    serde_json::from_slice(&RecordValue::encode(value.as_ref())?)?,
                ),
                _ => Ok(value.as_ref().and_then(T::from_value).cloned()),
            };
        }
        let paths = key.paths(root)?;
        let value = match maximum_bytes {
            Some(maximum_bytes) => read_json_bounded::<Option<T>>(&paths.current, maximum_bytes)?,
            None => read_json::<Option<T>>(&paths.current)?,
        }
        .flatten();
        self.residents()
            .insert(key, value.clone().map(T::into_value));
        Ok(value)
    }

    /// Keys of resident records that satisfy `select`.
    pub(super) fn resident_keys<K>(&self, select: impl Fn(RecordKey) -> Option<K>) -> Vec<K> {
        self.residents
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .filter_map(|key| select(*key))
            .collect()
    }

    /// Commits the after-images once their WAL frame is durable.
    pub(super) fn commit(
        &mut self,
        root: &Namespace,
        mut change: Change,
    ) -> Result<(), LocalCoreStateStoreError> {
        let work = root
            ._allocation
            .as_ref()
            .map(|allocation| allocation.work());
        if let Some(work) = work {
            self.adopt_native(work)?;
        }
        let next_sequence = self.sequence.checked_add(1).ok_or(
            crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
        )?;
        let mut install_allocation = work
            .map(|work| work.claim(0, 0, crate::WorkCounters::UNCHARGED))
            .transpose()?;
        let mut installs: Vec<(usize, bool)> = Vec::new();
        if let Some(work) = work {
            reserve(
                &mut installs,
                change.0.len(),
                work,
                install_allocation.as_mut().expect("install buffer"),
            )?;
            {
                let mut residents = self.residents();
                let needed = residents.0.len().checked_add(change.0.len()).ok_or(
                    crate::materializer::NativeTreeMaterializationError::Work(
                        crate::WorkError::Overflow,
                    ),
                )?;
                let Residents(values, slots) = &mut *residents;
                reserve_slots(values, needed, work, slots)?;
                let mut next = residents.0.len();
                for (ordinal, record) in change.0.iter().enumerate() {
                    let mut resident = None;
                    for (index, preceding) in residents.0.iter().enumerate() {
                        work.charge(crate::WorkCounters {
                            items_examined: 1,
                            ..Default::default()
                        })?;
                        if preceding.key == record.key {
                            resident = Some(index);
                            break;
                        }
                    }
                    let mut prior = None;
                    for (index, preceding) in change.0[..ordinal].iter().enumerate() {
                        work.charge(crate::WorkCounters {
                            items_examined: 1,
                            ..Default::default()
                        })?;
                        if preceding.key == record.key {
                            prior = Some(installs[index].0);
                            break;
                        }
                    }
                    let resident = resident.or(prior).unwrap_or_else(|| {
                        let index = next;
                        next += 1;
                        index
                    });
                    let mut dirty = prior.is_none();
                    for key in &self.dirty {
                        work.charge(crate::WorkCounters {
                            items_examined: 1,
                            ..Default::default()
                        })?;
                        if *key == record.key {
                            dirty = false;
                            break;
                        }
                    }
                    installs.push((resident, dirty));
                }
            }
            let needed = self
                .dirty
                .len()
                .checked_add(installs.iter().filter(|(_, dirty)| *dirty).count())
                .ok_or(crate::materializer::NativeTreeMaterializationError::Work(
                    crate::WorkError::Overflow,
                ))?;
            reserve_slots(&mut self.dirty, needed, work, &mut self.slots)?;
        }
        let mut buffer = LogBuffer {
            bytes: Vec::new(),
            allocation: work
                .map(|work| work.claim(0, 0, crate::WorkCounters::UNCHARGED))
                .transpose()?,
            work,
            failure: None,
        };
        if let Some(work) = work {
            reserve(
                &mut buffer.bytes,
                65,
                work,
                buffer.allocation.as_mut().expect("frame buffer"),
            )?;
            work.charge(crate::WorkCounters {
                bytes_copied: 65,
                ..Default::default()
            })?;
        }
        buffer.bytes.extend_from_slice(&[b' '; 65]);
        let encoded = serde_json::to_writer(
            &mut buffer,
            &LogFrame {
                epoch: self.epoch,
                sequence: self.sequence,
                change: &change,
            },
        );
        if let Some(error) = buffer.failure.take() {
            return Err(error);
        }
        encoded?;
        if let Some(work) = work {
            work.charge(crate::WorkCounters {
                bytes_hashed: (buffer.bytes.len() - 65) as u64,
                ..Default::default()
            })?;
        }
        let checksum = blake3::hash(&buffer.bytes[65..]).to_hex();
        if let Some(work) = work {
            let needed = buffer.bytes.len() + 1;
            reserve(
                &mut buffer.bytes,
                needed,
                work,
                buffer.allocation.as_mut().expect("frame buffer"),
            )?;
            work.charge(crate::WorkCounters {
                bytes_copied: 65,
                ..Default::default()
            })?;
        }
        buffer.bytes[..64].copy_from_slice(checksum.as_bytes());
        buffer.bytes.push(b'\n');
        let deferred = root.durability == Durability::Deferred;
        let new_length = self.length.checked_add(buffer.bytes.len() as u64).ok_or(
            crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
        )?;
        // Repair permission survives append failure, quota refusal and cancellation.
        let mut repair = work
            .map(|work| {
                work.reserve_work(crate::WorkCounters {
                    backend_write_operations: 1,
                    backend_read_operations: 1,
                    durability_operations: 1,
                    ..Default::default()
                })
            })
            .transpose()?;
        let mut flush = work
            .map(|work| {
                work.reserve_work(crate::WorkCounters {
                    durability_operations: u64::from(!deferred),
                    ..Default::default()
                })
            })
            .transpose()?;
        crash_point("before frame", self.flushed);
        let append = (|| {
            let mut position = 0;
            while position < buffer.bytes.len() {
                let mut credit = work
                    .map(|work| {
                        work.reserve_work(crate::WorkCounters {
                            backend_write_operations: 1,
                            object_bytes_written: (buffer.bytes.len() - position) as u64,
                            ..Default::default()
                        })
                    })
                    .transpose()?;
                let result = self.file.write(&buffer.bytes[position..]);
                let count = result.as_ref().copied().unwrap_or(0);
                let recorded = credit
                    .as_mut()
                    .map(|credit| {
                        credit.record_completed(crate::WorkCounters {
                            backend_write_operations: 1,
                            object_bytes_written: count as u64,
                            ..Default::default()
                        })
                    })
                    .transpose();
                let count = result?;
                recorded?;
                if count == 0 {
                    return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
                }
                position += count;
            }
            crash_point("frame written", self.flushed);
            if !deferred {
                let synced = self.file.sync_data();
                let recorded = flush
                    .as_mut()
                    .map(|flush| {
                        flush.record_completed(crate::WorkCounters {
                            durability_operations: 1,
                            ..Default::default()
                        })
                    })
                    .transpose();
                synced?;
                drop(recorded);
            }
            Ok::<_, LocalCoreStateStoreError>(())
        })();
        if let Err(error) = append {
            let truncated = self.file.set_len(self.length);
            drop(repair.as_mut().map(|repair| {
                repair.record_completed(crate::WorkCounters {
                    backend_write_operations: 1,
                    ..Default::default()
                })
            }));
            let repaired = truncated
                .and_then(|()| {
                    let sought = self.file.seek(SeekFrom::Start(self.length));
                    drop(repair.as_mut().map(|repair| {
                        repair.record_completed(crate::WorkCounters {
                            backend_read_operations: 1,
                            ..Default::default()
                        })
                    }));
                    sought
                })
                .and_then(|_| {
                    let synced = self.file.sync_data();
                    drop(repair.as_mut().map(|repair| {
                        repair.record_completed(crate::WorkCounters {
                            durability_operations: 1,
                            ..Default::default()
                        })
                    }));
                    synced
                });
            self.broken = repaired.is_err();
            return Err(error);
        }
        self.length = new_length;
        if !deferred {
            self.flushed = self.length;
            crash_point("frame flushed", self.flushed);
        }
        self.sequence = next_sequence;
        if work.is_some() {
            for (record, (resident, dirty)) in change.0.drain(..).zip(installs) {
                let key = record.key;
                let mut residents = self.residents();
                if resident < residents.0.len() {
                    residents.0[resident] = record;
                } else {
                    residents.0.push(record);
                }
                drop(residents);
                if dirty {
                    self.dirty.push(key);
                }
            }
        } else {
            self.apply(change);
        }
        Ok(())
    }

    /// Makes every appended frame durable.
    pub(super) fn flush(&mut self, root: &Namespace) -> Result<(), LocalCoreStateStoreError> {
        if self.flushed < self.length {
            if let Some(allocation) = &root._allocation {
                allocation.work().charge(crate::WorkCounters {
                    durability_operations: 1,
                    ..Default::default()
                })?;
            }
            self.file.sync_data()?;
            self.flushed = self.length;
            crash_point("frames flushed", self.flushed);
        }
        Ok(())
    }

    /// Makes a committed change resident; its files are written back later.
    pub(super) fn apply(&mut self, change: Change) {
        let mut residents = self
            .residents
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        for record in change.0 {
            let key = record.key;
            residents.insert(key, record.value);
            if !self.dirty.contains(&key) {
                self.dirty.push(key);
            }
        }
    }

    pub(super) fn checkpoint(&mut self, root: &Namespace) -> Result<(), LocalCoreStateStoreError> {
        let work = root
            ._allocation
            .as_ref()
            .map(|allocation| allocation.work());
        if let Some(work) = work {
            self.adopt_native(work)?;
        }
        self.flush(root)?;
        for key in &self.dirty {
            if let Some(work) = work {
                work.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            let paths = key.paths(root)?;
            let file = match work {
                Some(work) => {
                    crate::native_host::create_directories(&paths.directory, &mut &*work)?;
                    let file = materialization_open_options(
                        &paths.current,
                        &crate::native_host::NativeOpenOptions::ReadWriteCreate,
                        work,
                    )?;
                    work.charge(crate::WorkCounters {
                        backend_write_operations: 1,
                        ..Default::default()
                    })?;
                    file.set_len(0)?;
                    file
                }
                None => {
                    std::fs::create_dir_all(&paths.directory)?;
                    OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(&paths.current)?
                }
            };
            let mut writer = MaterializationWriter {
                file,
                work,
                encoded: true,
            };
            let residents = self.residents();
            let mut value = None;
            for record in &residents.0 {
                if let Some(work) = work {
                    work.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    })?;
                }
                if record.key == *key {
                    value = record.value.as_ref();
                    break;
                }
            }
            RecordValue::write_bare(value, &mut writer).map_err(measured_json_error)?;
            crash_point("record written", self.flushed);
            if let Some(work) = work {
                work.charge(crate::WorkCounters {
                    durability_operations: 1,
                    ..Default::default()
                })?;
            }
            writer.file.sync_all()?;
            crash_point("record flushed", self.flushed);
            match work {
                Some(work) => materialization_sync(&paths.directory, work)?,
                None => sync_directory(&paths.directory)?,
            }
        }
        let mut reset = work
            .map(|work| {
                work.reserve_work(crate::WorkCounters {
                    backend_write_operations: 1,
                    backend_read_operations: 1,
                    ..Default::default()
                })
            })
            .transpose()?;
        let truncated = self.file.set_len(0);
        let truncate_recorded = reset
            .as_mut()
            .map(|reset| {
                reset.record_completed(crate::WorkCounters {
                    backend_write_operations: 1,
                    ..Default::default()
                })
            })
            .transpose();
        truncated?;
        let sought = self.file.seek(SeekFrom::Start(0));
        let seek_recorded = reset
            .as_mut()
            .map(|reset| {
                reset.record_completed(crate::WorkCounters {
                    backend_read_operations: 1,
                    ..Default::default()
                })
            })
            .transpose();
        if let Err(error) = sought {
            self.broken = true;
            return Err(error.into());
        }
        self.length = 0;
        self.flushed = 0;
        crash_point("log truncated", self.flushed);
        self.epoch = Uuid::now_v7();
        self.sequence = 0;
        self.dirty.clear();
        truncate_recorded?;
        seek_recorded?;
        Ok(())
    }
}

/// Aborts the process at the configured crash point of a crash-atomicity test,
#[cfg(test)]
pub(super) fn crash_point(label: &str, flushed: u64) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static REACHED: AtomicU64 = AtomicU64::new(0);
    let Some(target) = std::env::var_os("ACYCLIC_CORE_STATE_CRASH_POINT") else {
        return;
    };
    let reached = REACHED.fetch_add(1, Ordering::SeqCst) + 1;
    if target.to_str().and_then(|target| target.parse().ok()) == Some(reached) {
        if let Some(report) = std::env::var_os("ACYCLIC_CORE_STATE_CRASH_REPORT") {
            let _ = std::fs::write(report, format!("{label}\n{flushed}"));
        }
        std::process::abort();
    }
}

#[cfg(not(test))]
pub(super) fn crash_point(_label: &str, _flushed: u64) {}

fn native_corrupt_log(
    mut paths: RecordPaths<'_>,
) -> Result<LocalCoreStateStoreError, LocalCoreStateStoreError> {
    let mut allocation = paths.allocation.take().expect("native record paths");
    let current = paths.current;
    drop((paths.directory, paths.previous, paths.temporary, paths.lock));
    allocation.substitute(current.capacity() as u64)?;
    Ok(LocalCoreStateStoreError::LazyWork(
        crate::LazyWorkspaceError::NativeRecordCorrupt(current, Some(allocation)),
    ))
}
