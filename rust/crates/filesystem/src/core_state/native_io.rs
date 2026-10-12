use super::*;

pub(super) fn reserve<T>(
    values: &mut Vec<T>,
    needed: usize,
    work: &crate::materializer::NativeMaterializationWork,
    held: &mut crate::materializer::NativeAllocation,
) -> Result<(), LocalCoreStateStoreError> {
    let prior = (values.capacity() * std::mem::size_of::<T>()) as u64;
    if held.bytes() < prior {
        let mut prefix = work.claim(prior - held.bytes(), 0, crate::WorkCounters::UNCHARGED)?;
        held.absorb(&mut prefix)?;
    }
    if needed <= values.capacity() {
        return Ok(());
    }
    let old = values.capacity();
    let next = needed
        .max(old.checked_mul(2).ok_or(
            crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
        )?)
        .max(1);
    let old_bytes = old.checked_mul(std::mem::size_of::<T>()).ok_or(
        crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
    )? as u64;
    let bytes = next.checked_mul(std::mem::size_of::<T>()).ok_or(
        crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
    )? as u64;
    let mut claim = work.claim(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED)?;
    let copying = work.reserve_work(crate::WorkCounters {
        bytes_copied: (values.len() * std::mem::size_of::<T>()) as u64,
        ..crate::WorkCounters::UNCHARGED
    })?;
    values
        .try_reserve_exact(next - values.len())
        .map_err(crate::materializer::NativeTreeMaterializationError::AllocationFailed)?;
    held.absorb(&mut claim)?;
    // The allocator has physically retired the old backing, not merely
    // overwritten its handles. Until here BOTH full capacities were funded.
    held.release(old_bytes)?;
    let actual = values
        .capacity()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::materializer::NativeTreeMaterializationError::Work(
            crate::WorkError::Overflow,
        ))? as u64;
    let capacity_result = if actual > bytes {
        let (mut excess, result) = work.record_completed_allocation(actual - bytes, None);
        held.absorb(&mut excess)?;
        result
    } else if actual < bytes {
        held.release(bytes - actual)
    } else {
        Ok(())
    };
    let copy_result = {
        let mut copy = copying;
        copy.record_completed(crate::WorkCounters {
            bytes_copied: (values.len() * std::mem::size_of::<T>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        })
    };
    capacity_result?;
    copy_result?;
    Ok(())
}

pub(super) fn reserve_slots<T>(
    values: &mut Vec<T>,
    needed: usize,
    work: &crate::materializer::NativeMaterializationWork,
    slot: &mut Option<crate::materializer::NativeAllocation>,
) -> Result<(), LocalCoreStateStoreError> {
    if slot.is_none() {
        *slot = Some(work.claim(0, 0, crate::WorkCounters::UNCHARGED)?);
    }
    reserve(
        values,
        needed,
        work,
        slot.as_mut().expect("capacity custody"),
    )
}
pub(super) fn measured_order<K: Ord, V>(
    values: &mut Vec<(K, V)>,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), LocalCoreStateStoreError> {
    // Preserve the public BTreeMap's last-write-wins and sorted encoding.
    for index in 1..values.len() {
        let mut position = index;
        while position != 0 {
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            if values[position - 1].0 <= values[position].0 {
                break;
            }
            work.charge(crate::WorkCounters {
                bytes_copied: (3 * std::mem::size_of::<(K, V)>()) as u64,
                ..Default::default()
            })?;
            values.swap(position - 1, position);
            position -= 1;
        }
    }
    let mut index = 1;
    while index < values.len() {
        work.charge(crate::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        if values[index - 1].0 == values[index].0 {
            work.charge(crate::WorkCounters {
                bytes_copied: ((values.len() - index) * std::mem::size_of::<(K, V)>()) as u64,
                ..Default::default()
            })?;
            values.remove(index - 1);
        } else {
            index += 1;
        }
    }
    Ok(())
}

impl RecordValue {
    pub(super) fn write_bare<W: Write>(
        value: Option<&Self>,
        writer: W,
    ) -> Result<(), serde_json::Error> {
        match value {
            None => serde_json::to_writer(writer, &()),
            Some(Self::Context(value)) => serde_json::to_writer(writer, value),
            Some(Self::Children(value)) => serde_json::to_writer(writer, value),
            Some(Self::ChildCount(value)) => serde_json::to_writer(writer, value),
            Some(Self::ChildIndex(value)) => serde_json::to_writer(writer, value),
            Some(Self::Lineage(value)) => serde_json::to_writer(writer, value),
            Some(Self::LazyWorkspace(value)) => serde_json::to_writer(writer, value),
            Some(Self::FlatContext(value)) => serde_json::to_writer(writer, value),
            Some(Self::FlatChildren(value)) => serde_json::to_writer(writer, value),
        }
    }
}

/// In-memory encoding is admitted at every actual Vec growth, before serde writes.
pub(super) struct LogBuffer<'a> {
    pub(super) bytes: Vec<u8>,
    pub(super) allocation: Option<crate::materializer::NativeAllocation>,
    pub(super) work: Option<&'a crate::materializer::NativeMaterializationWork>,
    pub(super) failure: Option<LocalCoreStateStoreError>,
}

impl Write for LogBuffer<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = (|| {
            let needed = self.bytes.len().checked_add(bytes.len()).ok_or(
                crate::materializer::NativeTreeMaterializationError::Work(
                    crate::WorkError::Overflow,
                ),
            )?;
            if let Some(work) = self.work {
                reserve(
                    &mut self.bytes,
                    needed,
                    work,
                    self.allocation.as_mut().expect("encoded buffer"),
                )?;
                work.charge(crate::WorkCounters {
                    bytes_encoded: bytes.len() as u64,
                    bytes_copied: bytes.len() as u64,
                    ..Default::default()
                })?;
            }
            self.bytes.extend_from_slice(bytes);
            Ok::<_, LocalCoreStateStoreError>(bytes.len())
        })();
        match result {
            Ok(count) => Ok(count),
            Err(error) => {
                self.failure = Some(error);
                Err(std::io::ErrorKind::Other.into())
            }
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn measured_record_path(
    root: &Path,
    leaf: &str,
    work: &crate::materializer::NativeMaterializationWork,
    allocation: &mut crate::materializer::NativeAllocation,
) -> Result<PathBuf, LocalCoreStateStoreError> {
    let mut owned = work.path(root, Some(Path::new(leaf)))?;
    owned.1.resize_retained(owned.0.capacity() as u64)?;
    allocation.absorb(&mut owned.1)?;
    Ok(owned.0)
}

pub(super) fn materialization_lock<T>(
    paths: RecordPaths<'_>,
    work: &crate::materializer::NativeMaterializationWork,
    action: impl FnOnce(&RecordPaths<'_>) -> Result<T, LocalCoreStateStoreError>,
) -> Result<T, LocalCoreStateStoreError> {
    let (paths, mut lock) = materialization_acquire_lock(paths, work)?;
    let result = action(&paths);
    let cleanup = lock
        .1
        .take()
        .ok_or(crate::materializer::NativeTreeMaterializationError::MissingCustody)?;
    let (unlocked, recorded) = cleanup.attempt(|| FileExt::unlock(&lock.0));
    match result {
        Err(error) => Err(error),
        Ok(value) => {
            recorded?;
            unlocked?;
            Ok(value)
        }
    }
}

pub(super) fn materialization_acquire_lock<'a>(
    mut paths: RecordPaths<'a>,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(RecordPaths<'a>, OwnerLock), LocalCoreStateStoreError> {
    crate::native_host::create_directories(&paths.directory, &mut &*work)?;
    let lock = materialization_open_options(
        &paths.lock,
        &crate::native_host::NativeOpenOptions::ReadWriteCreate,
        work,
    )?;
    let cleanup = work.reserve_cleanup_write()?;
    let deadline = Instant::now() + RECORD_LOCK_DEADLINE;
    loop {
        work.charge(crate::WorkCounters {
            backend_read_operations: 1,
            ..crate::WorkCounters::default()
        })?;
        match lock.try_lock_exclusive() {
            Ok(()) => break,
            Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) => {
                if Instant::now() >= deadline {
                    let mut allocation = paths.allocation.take().expect("native record paths");
                    drop((
                        paths.directory,
                        paths.current,
                        paths.previous,
                        paths.temporary,
                    ));
                    allocation.substitute(paths.lock.capacity() as u64)?;
                    return Err(LocalCoreStateStoreError::LazyWork(
                        crate::LazyWorkspaceError::NativeRecordLockTimeout(
                            paths.lock,
                            Some(allocation),
                        ),
                    ));
                }
                std::thread::sleep(RECORD_LOCK_RETRY);
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok((paths, OwnerLock(lock, Some(cleanup))))
}

pub(super) struct MaterializationReader<'a> {
    pub(super) file: File,
    pub(super) work: &'a crate::materializer::NativeMaterializationWork,
    pub(super) remaining_file_bytes: u64,
    pub(super) original_length: u64,
}

impl Read for MaterializationReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining_file_bytes == 0 {
            if crate::native_host::stat_file(&self.file, &mut &*self.work)
                .map_err(std::io::Error::other)?
                .len()
                != self.original_length
            {
                return Err(std::io::Error::other(
                    "materialization journal changed while reading",
                ));
            }
            return Ok(0);
        }
        let remaining = self.work.remaining().map_err(std::io::Error::other)?;
        let length = buffer
            .len()
            .min(
                usize::try_from(remaining.source_bytes_read.min(remaining.bytes_copied))
                    .unwrap_or(usize::MAX),
            )
            .min(usize::try_from(self.remaining_file_bytes).unwrap_or(usize::MAX));
        if length == 0 && !buffer.is_empty() {
            self.work
                .charge(crate::WorkCounters {
                    source_bytes_read: 1,
                    bytes_copied: 1,
                    ..Default::default()
                })
                .map_err(std::io::Error::other)?;
        }
        let count = crate::native_host::builtin_io(acyclic_native_runtime::NativeOperationKind::Read, (buffer
                .get_mut(..length)
                .ok_or_else(|| std::io::Error::other("invalid journal read bound"))?).len(), self.work, || std::io::Read::read(&mut &*(&self.file), buffer
                .get_mut(..length)
                .ok_or_else(|| std::io::Error::other("invalid journal read bound"))?))
        .map_err(std::io::Error::other)?;
        self.remaining_file_bytes -= count as u64;
        Ok(count)
    }
}

pub(super) fn measured_read_file<T: DeserializeOwned>(
    path: &Path,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<Option<(T, crate::materializer::NativeAllocation)>, LocalCoreStateStoreError> {
    let file = match materialization_open(path, false, work) {
        Ok(file) => file,
        Err(crate::materializer::NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    let length = crate::native_host::stat_file(&file, &mut &*work)?.len();
    let _buffer = {
        let claim_bytes = 64 * 1024;
        work.claim(
            claim_bytes,
            u64::from(claim_bytes != 0),
            crate::WorkCounters::UNCHARGED,
        )
    }?;
    let reader = std::io::BufReader::with_capacity(
        64 * 1024,
        MaterializationReader {
            file,
            work,
            remaining_file_bytes: length,
            original_length: length,
        },
    );
    measured_decode_reader(reader, length, work).map(Some)
}

pub(super) fn measured_json_error(error: serde_json::Error) -> LocalCoreStateStoreError {
    if !error.is_io() {
        return LocalCoreStateStoreError::Json(error);
    }
    let error = std::io::Error::from(error);
    if !error
        .get_ref()
        .is_some_and(|cause| cause.is::<crate::materializer::NativeTreeMaterializationError>())
    {
        return LocalCoreStateStoreError::Io(error);
    }
    let kind = error.kind();
    match error.into_inner() {
        Some(inner) => {
            match inner.downcast::<crate::materializer::NativeTreeMaterializationError>() {
                Ok(error) => LocalCoreStateStoreError::MaterializationWork(*error),
                Err(error) => LocalCoreStateStoreError::Io(std::io::Error::new(kind, error)),
            }
        }
        None => LocalCoreStateStoreError::Io(kind.into()),
    }
}

// Inputs are flat DTOs: scalars/arrays, owned Vec/String, or explicitly marked
// CoreStateFlatMap Vec pairs. Tree/Box/Arc reconstruction is separately metered.
pub(super) fn measured_decode_reader<T: DeserializeOwned>(
    reader: impl Read,
    encoded_bytes: u64,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(T, crate::materializer::NativeAllocation), LocalCoreStateStoreError> {
    // Serde's amortized string scratch is bounded by twice the encoded input.
    let scratch_bytes = encoded_bytes
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)
        .map_err(crate::materializer::NativeTreeMaterializationError::from)?
        .max(8);
    let _scratch = work.claim(
        scratch_bytes,
        1,
        crate::WorkCounters {
            bytes_copied: encoded_bytes,
            ..crate::WorkCounters::UNCHARGED
        },
    )?;
    let decode = MaterializationDecode {
        work,
        allocation: std::cell::RefCell::new(work.claim(0, 0, crate::WorkCounters::UNCHARGED)?),
        failure: std::cell::RefCell::new(None),
    };
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let result = T::deserialize(MaterializationDeserializer {
        inner: &mut deserializer,
        decode: &decode,
        flat_map: false,
    })
    .and_then(|value| deserializer.end().map(|()| value));
    if let Some(error) = decode.failure.borrow_mut().take() {
        return Err(LocalCoreStateStoreError::MaterializationWork(error));
    }
    let value = result.map_err(measured_json_error)?;
    drop(deserializer);
    Ok((value, decode.allocation.into_inner()))
}

pub(super) fn measured_decode_slice<T: DeserializeOwned>(
    bytes: &[u8],
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(T, crate::materializer::NativeAllocation), LocalCoreStateStoreError> {
    measured_decode_reader(std::io::Cursor::new(bytes), bytes.len() as u64, work)
}

pub(super) fn measured_read_record<T: DeserializeOwned>(
    paths: &RecordPaths,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<Option<(T, crate::materializer::NativeAllocation)>, LocalCoreStateStoreError> {
    match measured_read_file(&paths.current, work) {
        Ok(Some(value)) => Ok(Some(value)),
        Err(LocalCoreStateStoreError::Io(error)) => Err(LocalCoreStateStoreError::Io(error)),
        Err(LocalCoreStateStoreError::MaterializationWork(error)) => {
            Err(LocalCoreStateStoreError::MaterializationWork(error))
        }
        Err(LocalCoreStateStoreError::Json(error)) if error.is_io() => {
            Err(LocalCoreStateStoreError::Json(error))
        }
        current => match measured_read_file(&paths.previous, work)? {
            Some(value) => {
                materialization_copy(&paths.previous, &paths.current, work)?;
                materialization_sync(&paths.directory, work)?;
                Ok(Some(value))
            }
            None => current,
        },
    }
}

pub(super) struct MaterializationWriter<'a> {
    pub(super) file: File,
    pub(super) work: Option<&'a crate::materializer::NativeMaterializationWork>,
    pub(super) encoded: bool,
}

impl Write for MaterializationWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let Some(work) = self.work else {
            return self.file.write(bytes);
        };
        let remaining = work.remaining().map_err(std::io::Error::other)?;
        let length = bytes
            .len()
            .min(remaining.object_bytes_written as usize)
            .min(if self.encoded {
                remaining.bytes_encoded as usize
            } else {
                usize::MAX
            });
        if length == 0 && !bytes.is_empty() {
            work.charge(crate::WorkCounters {
                object_bytes_written: 1,
                bytes_encoded: u64::from(self.encoded),
                ..crate::WorkCounters::default()
            })
            .map_err(std::io::Error::other)?;
        }
        work.charge(crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::default()
        })
        .map_err(std::io::Error::other)?;
        let count = self.file.write(
            bytes
                .get(..length)
                .ok_or_else(|| std::io::Error::other("invalid journal write bound"))?,
        )?;
        work.spent(crate::WorkCounters {
            object_bytes_written: count as u64,
            bytes_encoded: if self.encoded { count as u64 } else { 0 },
            ..crate::WorkCounters::default()
        })
        .map_err(std::io::Error::other)?;
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn materialization_sync(
    path: &Path,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), LocalCoreStateStoreError> {
    let root = crate::native_host::HostRoot::open(path, &mut &*work)?;
    let directory = root.open_dir_held(Path::new(""), &mut &*work)?;
    crate::native_host::sync_directory_held(&directory, &mut &*work)?;
    Ok(())
}

pub(super) fn materialization_parent(
    path: &Path,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<cap_std::fs::Dir, crate::materializer::NativeTreeMaterializationError> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let held = crate::native_host::HostRoot::open(parent, &mut &*work)?;
    held.open_dir_held(Path::new(""), &mut &*work)
}

pub(super) fn materialization_open(
    path: &Path,
    truncate: bool,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<File, crate::materializer::NativeTreeMaterializationError> {
    let options = if truncate {
        crate::native_host::NativeOpenOptions::ReadWriteCreate
    } else {
        crate::native_host::NativeOpenOptions::Read
    };
    let file = materialization_open_options(path, &options, work)?;
    if truncate {
        crate::native_host::truncate_file(&file, 0, &mut &*work)?;
    }
    Ok(file)
}

pub(super) fn materialization_open_options(
    path: &Path,
    options: &crate::native_host::NativeOpenOptions,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<File, crate::materializer::NativeTreeMaterializationError> {
    let parent = materialization_parent(path, work)?;
    let leaf = path.file_name().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "journal file has no leaf")
    })?;
    crate::native_host::open_leaf(&parent, Path::new(leaf), options, &mut &*work)
}

pub(super) fn materialization_exists(
    path: &Path,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<bool, LocalCoreStateStoreError> {
    let parent = materialization_parent(path, work)?;
    let leaf = path.file_name().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "journal file has no leaf")
    })?;
    match crate::materializer::native_stat_held(&parent, Path::new(leaf), work) {
        Ok(_) => Ok(true),
        Err(crate::materializer::NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(windows))]
pub(super) fn materialization_rename(
    from: &Path,
    to: &Path,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), LocalCoreStateStoreError> {
    let parent = materialization_parent(from, work)?;
    let source = from.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "journal source has no leaf",
        )
    })?;
    let target = to.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "journal destination has no leaf",
        )
    })?;
    crate::native_host::rename_no_replace_held(
        &parent,
        Path::new(source),
        &parent,
        Path::new(target),
        &mut &*work,
    )?;
    Ok(())
}

pub(super) fn materialization_copy(
    from: &Path,
    to: &Path,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), LocalCoreStateStoreError> {
    let file = materialization_open(from, false, work)?;
    let length = crate::native_host::stat_file(&file, &mut &*work)?.len();
    let mut source = MaterializationReader {
        file,
        work,
        remaining_file_bytes: length,
        original_length: length,
    };
    let mut destination = MaterializationWriter {
        file: materialization_open(to, true, work)?,
        work: Some(work),
        encoded: false,
    };
    let mut bytes = [0; 64 * 1024];
    loop {
        let count = source.read(&mut bytes)?;
        if count == 0 {
            break;
        }
        destination.write_all(
            bytes
                .get(..count)
                .ok_or_else(|| std::io::Error::other("invalid journal copy length"))?,
        )?;
    }
    crate::native_host::sync_file_held(&destination.file, &mut &*work)?;
    Ok(())
}

pub(super) fn materialization_remove(
    path: &Path,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), LocalCoreStateStoreError> {
    let parent = materialization_parent(path, work)?;
    let leaf = path.file_name().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "journal file has no leaf")
    })?;
    match crate::native_host::remove_leaf(&parent, Path::new(leaf), false, &mut &*work) {
        Ok(()) => {}
        Err(crate::materializer::NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

pub(super) fn measured_write_record<T: Serialize>(
    paths: &RecordPaths,
    journal: &T,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), LocalCoreStateStoreError> {
    let mut writer = MaterializationWriter {
        file: materialization_open(&paths.temporary, true, work)?,
        work: Some(work),
        encoded: true,
    };
    let encoded = serde_json::to_writer(&mut writer, journal);
    encoded.map_err(measured_json_error)?;
    crate::native_host::sync_file_held(&writer.file, &mut &*work)?;
    drop(writer);
    materialization_remove(&paths.previous, work)?;
    #[cfg(windows)]
    {
        let previous = if materialization_exists(&paths.current, work)? {
            &paths.current
        } else {
            &paths.temporary
        };
        materialization_copy(previous, &paths.previous, work)?;
        materialization_copy(&paths.temporary, &paths.current, work)?;
        materialization_remove(&paths.temporary, work)?;
    }
    #[cfg(not(windows))]
    {
        if materialization_exists(&paths.current, work)? {
            materialization_rename(&paths.current, &paths.previous, work)?;
        }
        materialization_rename(&paths.temporary, &paths.current, work)?;
        materialization_sync(&paths.directory, work)?;
    }
    materialization_remove(&paths.previous, work)?;
    Ok(())
}
