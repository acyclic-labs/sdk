use super::*;

pub(crate) fn reserve<T>(
    values: &mut Vec<T>,
    needed: usize,
    work: &crate::materializer::NativeMaterializationWork,
    held: &mut crate::materializer::NativeAllocation,
) -> Result<(), crate::NativeTreeMaterializationError> {
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
    .map_err(Into::into)
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
        }
    }
}

/// In-memory encoding is admitted at every actual Vec growth, before serde writes.
pub(super) struct LogBuffer {
    pub(super) bytes: Vec<u8>,
    pub(super) allocation: Option<crate::materializer::NativeAllocation>,
    pub(super) failure: Option<LocalCoreStateStoreError>,
}

impl Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = (|| {
            let needed = self.bytes.len().checked_add(bytes.len()).ok_or(
                crate::materializer::NativeTreeMaterializationError::Work(
                    crate::WorkError::Overflow,
                ),
            )?;
            if let Some(allocation) = &mut self.allocation {
                let work = allocation.work().clone();
                reserve(&mut self.bytes, needed, &work, allocation)?;
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
    owned.1.resize(owned.0.capacity() as u64)?;
    allocation.absorb(&mut owned.1)?;
    Ok(owned.0)
}

pub(super) struct MaterializationReader<'a> {
    pub(super) file: &'a File,
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
            .min(64 * 1024)
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
        let count = crate::native_host::read_builtin_file(
            &self.file,
            buffer
                .get_mut(..length)
                .ok_or_else(|| std::io::Error::other("invalid journal read bound"))?,
            self.work,
        )
        .map_err(std::io::Error::other)?;
        self.remaining_file_bytes -= count as u64;
        Ok(count)
    }
}

pub(super) fn measured_read_file<T: DeserializeOwned>(
    path: &Path,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<Option<(T, crate::materializer::NativeAllocation)>, LocalCoreStateStoreError> {
    let file = match materialization_open_options(
        path,
        &crate::native_host::NativeOpenOptions::Read,
        work,
    ) {
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
            file: &file,
            work,
            remaining_file_bytes: length,
            original_length: length,
        },
    );
    measured_decode_reader(reader, length, work).map(Some)
}

// SDK payloads are funded before decode; std tree backing remains opaque.
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
        tree_set: false,
    })
    .and_then(|value| deserializer.end().map(|()| value));
    if let Some(error) = decode.failure.borrow_mut().take() {
        return Err(LocalCoreStateStoreError::from(error));
    }
    let value = result?;
    drop(deserializer);
    Ok((value, decode.allocation.into_inner()))
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
