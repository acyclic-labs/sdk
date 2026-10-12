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
            MaterializationReader {
                file: &file,
                work,
                remaining_file_bytes: length,
                original_length: length,
            }
            .read_exact(&mut bytes)?;
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
            if let Some(work) = work {
                // The decoded records retire before the frame's allocation.
                let mut frame_allocation = allocation.expect("native frame allocation");
                for mut record in frame.change.0 {
                    let owned_bytes = record
                        .value
                        .as_ref()
                        .map(|value| native_records::native_record_owned_bytes(value, work))
                        .transpose()?
                        .unwrap_or(0);
                    let (index, dirty) = log.admit_lazy_slots(record.key, work)?;
                    let key = record.key;
                    record.allocation = Some(frame_allocation.split_off(owned_bytes)?);
                    let mut residents = log.residents();
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
            } else {
                log.apply(frame.change, None)?;
            }
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
