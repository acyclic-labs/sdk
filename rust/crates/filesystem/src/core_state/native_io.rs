use super::*;

pub(crate) fn reserve<T>(
    values: &mut Vec<T>,
    needed: usize,
    work: &crate::materializer::NativeMaterializationWork,
    held: &mut crate::materializer::NativeAllocation,
) -> Result<(), crate::NativeTreeMaterializationError> {
    work.admit(crate::WorkCounters::UNCHARGED)?;
    let width = std::mem::size_of::<T>();
    let prior = values
        .capacity()
        .checked_mul(width)
        .ok_or(crate::WorkError::Overflow)? as u64;
    if held.work().shares_allocation_context(work) {
        if held.bytes() < prior {
            let mut prefix = work.claim(prior - held.bytes(), 0, crate::WorkCounters::UNCHARGED)?;
            held.absorb(&mut prefix)?;
        }
        if needed <= values.capacity() {
            return Ok(());
        }
    }
    let capacity = if needed <= values.capacity() {
        values.capacity()
    } else {
        needed
            .max(
                values
                    .capacity()
                    .checked_mul(2)
                    .ok_or(crate::WorkError::Overflow)?,
            )
            .max(1)
    };
    let copied = crate::WorkCounters {
        bytes_copied: values
            .len()
            .checked_mul(width)
            .ok_or(crate::WorkError::Overflow)? as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    let copying = work.reserve_work(copied)?;
    let (mut replacement, allocation) = crate::native_host::buffer(capacity, &mut &*work)?;
    replacement.append(values);
    // Destroy the prior backing before retiring its original ledger's custody.
    drop(std::mem::replace(values, replacement));
    *held = allocation;
    copying.record_completed(copied)
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
        slot.as_mut()
            .ok_or(crate::NativeTreeMaterializationError::MissingCustody)?,
    )
    .map_err(Into::into)
}

impl RecordValue {
    /// Encode one complete bare checkpoint record before the file barrier.
    pub(super) fn write_checkpoint(
        value: Option<&Self>,
        writer: &mut impl Write,
        work: Option<&crate::materializer::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        if let Some(work) = work {
            let mut encoded = LogBuffer {
                bytes: Vec::new(),
                allocation: Some(work.claim(0, 0, crate::WorkCounters::UNCHARGED)?),
            };
            Self::write_bare(value, &mut encoded)?;
            writer.write_all(&encoded.bytes)?;
        } else {
            let mut buffered = std::io::BufWriter::with_capacity(8192, writer);
            Self::write_bare(value, &mut buffered)?;
            buffered.flush()?;
        }
        Ok(())
    }

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
            Ok::<_, crate::NativeTreeMaterializationError>(bytes.len())
        })();
        result.map_err(std::io::Error::other)
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
            if crate::native_host::stat_file(self.file, &mut &*self.work)
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
        let buffer = buffer.get_mut(..length).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "read exceeds buffer")
        })?;
        let count = crate::native_host::file_transfer(
            acyclic_native_runtime::NativeOperationKind::Read,
            length,
            self.work,
            || self.file.read(buffer),
        )
        .map_err(std::io::Error::other)?;
        self.remaining_file_bytes -= count as u64;
        Ok(count)
    }
}

pub(super) fn read_file<T: DeserializeOwned>(
    path: &Path,
    work: &crate::materializer::NativeMaterializationWork,
    maximum_bytes: Option<u64>,
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
    if maximum_bytes.is_some_and(|maximum| length > maximum) {
        return Err(LocalCoreStateStoreError::Integrity);
    }
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
    let result = T::deserialize(Funded {
        inner: &mut deserializer,
        decode: &decode,
        kind: DecodeKind::Borrowed,
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
    crate::native_host::HostRoot::open(parent, &mut &*work).map(|root| root.directory)
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
