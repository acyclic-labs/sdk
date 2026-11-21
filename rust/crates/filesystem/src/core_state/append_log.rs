use super::*;

/// Writes one encoded batch at its committed end, leaving publication to the
/// caller. The same durable-write trace covers each attempted physical write.
pub(super) fn write_append_batch(
    offset: u64,
    buffer: &[u8],
    work: Option<&crate::NativeMaterializationWork>,
    mut write_at: impl FnMut(u64, &[u8]) -> std::io::Result<usize>,
) -> Result<(), LocalCoreStateStoreError> {
    offset
        .checked_add(buffer.len() as u64)
        .ok_or(crate::WorkError::Overflow)
        .map_err(crate::NativeTreeMaterializationError::from)?;
    let mut position = 0;
    while position < buffer.len() {
        let mut credit = work
            .map(|work| {
                work.reserve_work(crate::WorkCounters {
                    backend_write_operations: 1,
                    object_bytes_written: (buffer.len() - position) as u64,
                    ..Default::default()
                })
            })
            .transpose()?;
        let mut recorded = Ok(None);
        let result = acyclic_native_runtime::write_file_with(&buffer[position..], |bytes| {
            let result = write_at(offset + position as u64, bytes);
            let count = result.as_ref().copied().unwrap_or(0);
            recorded = credit
                .as_mut()
                .map(|credit| {
                    credit.record_completed(crate::WorkCounters {
                        backend_write_operations: 1,
                        object_bytes_written: count as u64,
                        ..Default::default()
                    })
                })
                .transpose();
            result
        });
        let count = result?;
        recorded?;
        if count == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
        }
        position += count;
    }
    Ok(())
}

pub(super) fn open_log_file(
    root: &Namespace,
    paths: &RecordPaths<'_>,
    work: Option<&crate::NativeMaterializationWork>,
) -> Result<File, LocalCoreStateStoreError> {
    let file = if root.inspection {
        match work {
            Some(work) => {
                let parent = crate::native_host::HostRoot::open(paths.directory(), &mut &*work)?;
                materialization_open_at(
                    &parent.directory(),
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
            &parent.directory(),
            &paths.current,
            &crate::native_host::NativeOpenOptions::ReadWriteCreateNew,
            work,
        ) {
            Ok(file) => {
                crate::native_host::sync_directory_held(&parent.directory(), &mut &*work)?;
                file
            }
            Err(crate::materializer::NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists =>
            {
                materialization_open_at(
                    &parent.directory(),
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
                sync_parent(paths.directory(), NativeDurability::Full)?;
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

pub(super) fn read_log_bytes(
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

pub(super) fn checked_log_frame<'a>(
    format: acyclic_log_frame::LogFrame,
    bytes: &'a [u8],
    offset: &mut usize,
    work: Option<&crate::NativeMaterializationWork>,
) -> Result<Option<&'a [u8]>, LocalCoreStateStoreError> {
    let available = bytes
        .get(*offset..)
        .ok_or(LocalCoreStateStoreError::Integrity)?;
    let Some(length) = format.body_length(available) else {
        return Ok(None);
    };
    let Some(extent) = format.frame_bytes(length) else {
        return Ok(None);
    };
    if extent > available.len() as u64 {
        return Ok(None);
    }
    if let Some(work) = work {
        work.charge(crate::WorkCounters {
            items_examined: 1,
            bytes_hashed: length + (format.domain.len() + format.prefix_bytes()) as u64,
            ..Default::default()
        })?;
    }
    let Some((frame, rest)) = format.decode_prefix(available) else {
        return Ok(None);
    };
    *offset = bytes.len() - rest.len();
    Ok(Some(frame))
}

impl LogBuffer {
    /// Adds one frame to a funded append buffer.
    pub(super) fn push_frame(
        &mut self,
        format: acyclic_log_frame::LogFrame,
        payload: &impl Serialize,
    ) -> Result<(), LocalCoreStateStoreError> {
        let start = self.bytes.len();
        let encoded = self.encode_payload(start, format, payload);
        if encoded.is_err() {
            self.bytes.truncate(start);
        }
        encoded
    }

    fn encode_payload(
        &mut self,
        start: usize,
        format: acyclic_log_frame::LogFrame,
        payload: &impl Serialize,
    ) -> Result<(), LocalCoreStateStoreError> {
        let prefix = format.prefix_bytes();
        let checksum = acyclic_log_frame::LogFrame::CHECKSUM_BYTES;
        let body_start = start
            .checked_add(prefix)
            .ok_or(LocalCoreStateStoreError::Integrity)?;
        let work = self
            .allocation
            .as_ref()
            .map(|allocation| allocation.work().clone());
        if let Some(work) = &work {
            reserve(
                &mut self.bytes,
                body_start,
                work,
                self.allocation
                    .as_mut()
                    .ok_or(LocalCoreStateStoreError::Integrity)?,
            )?;
            work.charge(crate::WorkCounters {
                bytes_copied: prefix as u64,
                ..Default::default()
            })?;
        }
        self.bytes.resize(body_start, 0);
        serde_json::to_writer(&mut *self, payload)?;
        if let Some(work) = &work {
            let needed = self
                .bytes
                .len()
                .checked_add(checksum)
                .ok_or(LocalCoreStateStoreError::Integrity)?;
            reserve(
                &mut self.bytes,
                needed,
                work,
                self.allocation
                    .as_mut()
                    .ok_or(LocalCoreStateStoreError::Integrity)?,
            )?;
            work.charge(crate::WorkCounters {
                bytes_hashed: (self.bytes.len() - start + format.domain.len()) as u64,
                bytes_copied: (prefix + checksum) as u64,
                ..Default::default()
            })?;
        }
        format.seal(&mut self.bytes, start)?;
        Ok(())
    }
}
