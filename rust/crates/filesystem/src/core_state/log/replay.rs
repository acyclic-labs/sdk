use super::*;

impl CoreLog {
    pub(in crate::core_state) fn open(
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
                        paths.sync()?;
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
            let length = crate::native_host::stat_file(&file, &mut &*work)?.len();
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
                Err(error @ LocalCoreStateStoreError::LazyWork(_)) => return Err(error),
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
}

fn native_corrupt_log(
    mut paths: RecordPaths<'_>,
) -> Result<LocalCoreStateStoreError, LocalCoreStateStoreError> {
    let mut allocation = paths.allocation.take().expect("native record paths");
    let current = paths.current;
    drop((paths.directory, paths.previous, paths.temporary, paths.lock));
    allocation.resize(current.capacity() as u64)?;
    Ok(LocalCoreStateStoreError::LazyWork(
        crate::LazyWorkspaceError::NativeRecordCorrupt(current, Some(allocation)),
    ))
}
