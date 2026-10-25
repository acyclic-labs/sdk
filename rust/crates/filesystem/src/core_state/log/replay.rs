use super::*;

impl CoreLog {
    pub(in crate::core_state) fn open(
        root: &Namespace,
        residents: Arc<RwLock<Residents>>,
    ) -> Result<Self, LocalCoreStateStoreError> {
        Self::open_prefix(root, residents, None)
    }

    pub(in crate::core_state) fn open_prefix(
        root: &Namespace,
        residents: Arc<RwLock<Residents>>,
        maximum: Option<u64>,
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
        let file = open_log_file(root, &paths, work)?;
        let buffer = read_log_bytes(&file, work, maximum)?;
        let bytes = &buffer.bytes;
        {
            let mut previous = residents.write().unwrap_or_else(PoisonError::into_inner);
            if let Some(work) = work {
                for record in previous.iter() {
                    let children = match &record.value {
                        Some(RecordValue::Context(context)) => context.roots.len(),
                        Some(RecordValue::Children(children)) => children.0.len(),
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
        let published = (!root.inspection && matches!(&root.admission, Admission::Owned(_)))
            .then(|| Arc::clone(&residents));
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
        let file_control = work.map(super::super::group::file_control).transpose()?;
        let mut log = Self {
            file: Arc::new(file),
            length,
            flushed: 0,
            epoch: crate::identity::fresh_time_uuid(),
            sequence: 0,
            residents: replay_residents,
            dirty: Vec::new(),
            broken: false,
            slots: None,
            residents_control: None,
            file_control,
        };
        log.replay_frames(bytes, paths, work)?;
        if !root.inspection && length != 0 {
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
    fn replay_frames(
        &mut self,
        bytes: &[u8],
        paths: RecordPaths<'_>,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        let mut epoch = None;
        let mut sequence = 0;
        let mut line_start = 0;
        while line_start < bytes.len() {
            let Some(frame) = checked_log_frame(bytes, &mut line_start, work)? else {
                break;
            };
            let (frame, allocation): (LogFrame<Change>, _) = match work {
                Some(work) => match measured_decode_reader(
                    std::io::Cursor::new(frame),
                    frame.len() as u64,
                    work,
                ) {
                    Ok((frame, allocation)) => (frame, Some(allocation)),
                    Err(error @ LocalCoreStateStoreError::LazyWork(_)) => return Err(error),
                    Err(_) => return Err(native_corrupt_log(paths)?),
                },
                None => (
                    serde_json::from_slice(frame)
                        .map_err(|_| LocalCoreStateStoreError::Corrupt(paths.current.clone()))?,
                    None,
                ),
            };
            // Payload drops before its allocation on every early exit.
            let frame = frame;
            if *epoch.get_or_insert(frame.epoch) != frame.epoch || frame.sequence != sequence {
                break;
            }
            self.apply_replayed(frame.change, allocation, work)?;
            sequence += 1;
        }
        Ok(())
    }

    fn apply_replayed(
        &mut self,
        change: Change,
        allocation: Option<crate::NativeAllocation>,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        if let Some(work) = work {
            // The decoded records retire before the frame's allocation.
            let mut frame_allocation =
                allocation.ok_or(crate::NativeTreeMaterializationError::MissingCustody)?;
            for mut record in change.0 {
                let owned_bytes = record
                    .value
                    .as_ref()
                    .map(|value| native_records::native_record_owned_bytes(value, work))
                    .transpose()?
                    .unwrap_or(0);
                let installation = self.admit_lazy_slots(record.key, work)?;
                let key = record.key;
                record.allocation = Some(frame_allocation.split_off(owned_bytes)?);
                let prepared = PreparedResident::Native(AdoptedResident::new(record, work)?);
                let mut residents = self.residents();
                let mut examined = 0;
                let dirty = residents.install(prepared, Some(self.epoch), &mut examined);
                installation.record_completed(crate::WorkCounters {
                    items_examined: examined,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                let dirty = dirty?;
                drop(residents);
                if dirty {
                    self.dirty.push(key);
                }
            }
        } else {
            let change = PreparedChange::new(change, None)?;
            let installation = self.admit_installation(change.change(), None)?;
            self.apply(change, installation.as_ref())?;
        }
        Ok(())
    }
}

fn native_corrupt_log(
    mut paths: RecordPaths<'_>,
) -> Result<LocalCoreStateStoreError, LocalCoreStateStoreError> {
    let mut allocation = paths
        .allocation
        .take()
        .ok_or(crate::NativeTreeMaterializationError::MissingCustody)?;
    let current = paths.current;
    drop((paths.previous, paths.temporary, paths.lock));
    allocation.resize(current.capacity() as u64)?;
    Ok(LocalCoreStateStoreError::LazyWork(
        crate::LazyWorkspaceError::NativeRecordCorrupt(current, Some(allocation)),
    ))
}

fn open_log_file(
    root: &Namespace,
    paths: &RecordPaths<'_>,
    work: Option<&crate::NativeMaterializationWork>,
) -> Result<File, LocalCoreStateStoreError> {
    let file = if root.inspection {
        match work {
            Some(work) => {
                let parent = crate::native_host::HostRoot::open(paths.directory(), &mut &*work)?;
                materialization_open_at(
                    &parent.directory,
                    &paths.current,
                    &crate::native_host::NativeOpenOptions::Read,
                    work,
                )?
            }
            None => File::open(&paths.current)?,
        }
    } else if let Some(work) = work {
        let parent = crate::native_host::create_directories(paths.directory(), &mut &*work)?;
        match materialization_open_at(
            &parent.directory,
            &paths.current,
            &crate::native_host::NativeOpenOptions::ReadWriteCreateNew,
            work,
        ) {
            Ok(file) => {
                crate::native_host::sync_directory_held(&parent.directory, &mut &*work)?;
                file
            }
            Err(crate::materializer::NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists =>
            {
                materialization_open_at(
                    &parent.directory,
                    &paths.current,
                    &crate::native_host::NativeOpenOptions::ReadWrite,
                    work,
                )?
            }
            Err(error) => return Err(error.into()),
        }
    } else {
        std::fs::create_dir_all(paths.directory())?;
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&paths.current)
        {
            Ok(file) => {
                sync_directory(paths.directory())?;
                file
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
                .read(true)
                .write(true)
                .open(&paths.current)?,
            Err(error) => return Err(error.into()),
        }
    };
    Ok(file)
}

fn read_log_bytes(
    mut file: &File,
    work: Option<&crate::NativeMaterializationWork>,
    maximum: Option<u64>,
) -> Result<LogBuffer, LocalCoreStateStoreError> {
    let mut bytes_allocation = work
        .map(|work| work.claim(0, 0, crate::WorkCounters::UNCHARGED))
        .transpose()?;
    let mut bytes = Vec::new();
    if let Some(work) = work {
        let original_length = crate::native_host::stat_file(file, &mut &*work)?.len();
        let length = maximum.map_or(original_length, |maximum| original_length.min(maximum));
        let size = usize::try_from(length).map_err(|_| {
            crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow)
        })?;
        reserve(
            &mut bytes,
            size,
            work,
            bytes_allocation
                .as_mut()
                .ok_or(crate::NativeTreeMaterializationError::MissingCustody)?,
        )?;
        work.charge(crate::WorkCounters {
            bytes_copied: length,
            ..Default::default()
        })?;
        bytes.resize(size, 0);
        MaterializationReader {
            file,
            work,
            remaining_file_bytes: length,
            original_length,
        }
        .read_exact(&mut bytes)?;
    } else {
        match maximum {
            Some(maximum) => {
                file.take(maximum).read_to_end(&mut bytes)?;
            }
            None => {
                (&mut file).read_to_end(&mut bytes)?;
            }
        }
    }
    Ok(LogBuffer {
        bytes,
        allocation: bytes_allocation,
    })
}

fn checked_log_frame<'a>(
    bytes: &'a [u8],
    line_start: &mut usize,
    work: Option<&crate::NativeMaterializationWork>,
) -> Result<Option<&'a [u8]>, LocalCoreStateStoreError> {
    let mut line_end = *line_start;
    while let Some(&byte) = bytes.get(line_end) {
        if let Some(work) = work {
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
        }
        line_end += 1;
        if byte == b'\n' {
            break;
        }
    }
    let line = bytes
        .get(*line_start..line_end)
        .ok_or(LocalCoreStateStoreError::Integrity)?;
    *line_start = line_end;
    if let Some(work) = work {
        work.charge(crate::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
    }
    let Some(line) = line.strip_suffix(b"\n") else {
        return Ok(None);
    };
    let Some((checksum, frame)) = line.split_first_chunk::<64>() else {
        return Ok(None);
    };
    let Some(frame) = frame.strip_prefix(b" ") else {
        return Ok(None);
    };
    if let Some(work) = work {
        work.charge(crate::WorkCounters {
            bytes_hashed: frame.len() as u64,
            ..Default::default()
        })?;
    }
    if blake3::hash(frame).to_hex().as_bytes() != checksum {
        return Ok(None);
    }
    Ok(Some(frame))
}
