use super::*;

#[derive(Debug)]
pub(crate) struct NativeFilePayload {
    pub(crate) file: File,
    // Fields retire in declaration order: physical File before its claim.
    pub(crate) _allocation: Option<ReadAllocation>,
}

impl std::ops::Deref for NativeFilePayload {
    type Target = File;
    fn deref(&self) -> &File {
        &self.file
    }
}

#[cfg(unix)]
impl std::os::fd::AsFd for NativeFilePayload {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.file)
    }
}

/// One file owned by the native I/O runtime.
pub struct NativeFile {
    pub(crate) file: Arc<NativeFilePayload>,
    pub(crate) health: Arc<FileHealth>,
    #[cfg(windows)]
    pub(crate) overlapped: bool,
}

/// Accounting supplied when importing a native handle.
pub enum NativeFileMeter<'a> {
    /// The caller retains responsibility for releasing the handle allocation.
    Borrowed(&'a dyn NativeReadMeter),
    /// The handle retains and releases its original allocation claim.
    Owned(Arc<dyn NativeReadMeter>),
}

impl NativeFile {
    /// Imports an authorized handle, returning it unchanged when admission fails.
    pub fn from_file(
        file: File,
        accounting: Option<NativeFileMeter<'_>>,
    ) -> Result<Self, (io::Error, File)> {
        let (meter, owner) = match &accounting {
            Some(NativeFileMeter::Borrowed(meter)) => (Some(*meter), None),
            Some(NativeFileMeter::Owned(meter)) => (Some(meter.as_ref()), Some(meter)),
            None => (None, None),
        };
        let admitted = (|| {
            if let Some(meter) = meter {
                meter.check()?;
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                if FILE_HEALTH.get().is_none() {
                    return Err(io::ErrorKind::NotConnected.into());
                }
                #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
                if owner.is_some() {
                    return Err(io::ErrorKind::Unsupported.into());
                }
            }
            let bytes = std::mem::size_of::<NativeFilePayload>();
            match owner {
                Some(owner) => ReadAllocation::new(owner, bytes).map(Some),
                None => {
                    if let Some(meter) = meter {
                        meter.allocate(bytes as u64)?;
                    }
                    Ok(None)
                }
            }
        })();
        let allocation = match admitted {
            Ok(allocation) => allocation,
            Err(error) => return Err((error, file)),
        };
        let payload = Arc::new(NativeFilePayload {
            file,
            _allocation: allocation,
        });
        let health = (|| {
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            {
                shared_file_health(&payload, meter)
            }
            #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
            {
                let mut retained = 0;
                let result = (|| {
                    if let Some(meter) = meter {
                        let bytes =
                            std::mem::size_of::<Mutex<Option<Arc<OperationFence>>>>() as u64;
                        meter.allocate(bytes)?;
                        retained += bytes;
                    }
                    let tail = Arc::new(Mutex::new(None));
                    if let Some(meter) = meter {
                        let bytes = std::mem::size_of::<AtomicBool>() as u64;
                        meter.allocate(bytes)?;
                        retained += bytes;
                    }
                    let uncertain = Arc::new(AtomicBool::new(false));
                    if let Some(meter) = meter {
                        let bytes = std::mem::size_of::<FileHealth>() as u64;
                        meter.allocate(bytes)?;
                        retained += bytes;
                    }
                    Ok(Arc::new(FileHealth {
                        tail,
                        uncertain,
                        poison_anchor: Mutex::new(None),
                    }))
                })();
                if result.is_err() {
                    if let Some(meter) = meter {
                        meter.release(retained);
                    }
                }
                result
            }
        })();
        match health {
            Ok(health) => Ok(Self {
                file: payload,
                health,
                #[cfg(windows)]
                overlapped: false,
            }),
            Err(error) => {
                let NativeFilePayload { file, _allocation } = Arc::try_unwrap(payload)
                    .ok()
                    .expect("unpublished native handle");
                if _allocation.is_none() {
                    if let Some(meter) = meter {
                        meter.release(std::mem::size_of::<NativeFilePayload>() as u64);
                    }
                }
                drop(_allocation);
                Err((error, file))
            }
        }
    }

    /// Logical handle storage owned by this capability, excluding the shared runtime health/sequencing control root.
    #[must_use]
    pub fn logical_owned_bytes(&self) -> u64 {
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let health = 0;
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let health = std::mem::size_of::<Mutex<Option<Arc<OperationFence>>>>()
            + std::mem::size_of::<AtomicBool>()
            + std::mem::size_of::<FileHealth>();
        (std::mem::size_of::<NativeFilePayload>() + health) as u64
    }

    /// Transfers a Windows handle already opened for overlapped I/O.
    #[cfg(windows)]
    #[allow(
        unsafe_code,
        reason = "the caller must uphold the overlapped-handle ownership contract"
    )]
    pub unsafe fn from_overlapped_file_unchecked(
        file: File,
        meter: Option<NativeFileMeter<'_>>,
    ) -> Result<Self, (io::Error, File)> {
        let mut native = Self::from_file(file, meter)?;
        native.overlapped = true;
        Ok(native)
    }

    /// Changes logical file length in this file's operation order.
    pub fn set_len_async(
        &self,
        size: u64,
        meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> io::Result<NativeCompletion<()>> {
        self.control_async(
            move |file, meter| match meter {
                Some(meter) => set_len(file, size, meter),
                None => file.set_len(size),
            },
            meter,
        )
    }

    /// Runs a platform-specific handle control in this file's operation order.
    pub fn control_async<F>(
        &self,
        operation: F,
        meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> io::Result<NativeCompletion<()>>
    where
        F: FnOnce(&File, Option<&dyn NativeReadMeter>) -> io::Result<()> + Send + 'static,
    {
        if let Some(meter) = &meter {
            meter.check()?;
        }
        let allocation = meter
            .as_ref()
            .map(|meter| ReadAllocation::new(meter, std::mem::size_of::<F>()))
            .transpose()?;
        self.submit_unit(
            NativeUnitOperation::Control {
                operation: Box::new(operation),
                _allocation: allocation,
            },
            meter,
        )
    }

    /// Borrows the already-held file for measured scalar metadata operations.
    #[must_use]
    pub fn as_file(&self) -> &File {
        &self.file
    }

    /// Reads caller-owned storage; `AsMut` must expose the same initialized slice without allocating.
    pub fn read_prepared_bounded_async<B>(
        &self,
        offset: u64,
        mut storage: B,
        length: usize,
        require_full: bool,
        meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<NativeCompletion<(B, usize)>>
    where
        B: AsMut<[u8]> + Send + 'static,
    {
        meter.check()?;
        meter.admit_bytes(length)?;
        if storage.as_mut().len() < length {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        validate_prepared_read_range(offset, length)?;
        let file = Arc::clone(&self.file);
        let observer = Arc::clone(&meter);
        #[cfg(windows)]
        let overlapped = self.overlapped;
        task_completion(
            move || {
                let destination = storage
                    .as_mut()
                    .get_mut(..length)
                    .ok_or(io::ErrorKind::InvalidInput)?;
                read_prepared_in_place(
                    &file,
                    offset,
                    destination,
                    require_full,
                    observer.as_ref(),
                    #[cfg(windows)]
                    overlapped,
                )
                .map(|filled| (storage, filled))
            },
            Some(meter),
            None,
            Some(Arc::clone(&self.health)),
            &NATIVE_WORKERS,
        )
    }

    /// Reads a bounded range through this file's ordered worker queue.
    pub fn read_bounded_async(
        &self,
        offset: u64,
        length: usize,
        meter: Arc<dyn NativeReadMeter>,
        require_full: bool,
    ) -> io::Result<NativeCompletion<NativeReadResult>> {
        meter.check()?;
        meter.admit_bytes(length)?;
        if length == 0 {
            return Ok(NativeCompletion::ready(Ok(NativeReadResult {
                bytes: Bytes::new(),
                buffer_capacity: 0,
                _allocation: None,
            })));
        }
        validate_prepared_read_range(offset, length)?;
        let file = Arc::clone(&self.file);
        let observer = Arc::clone(&meter);
        #[cfg(windows)]
        let overlapped = self.overlapped;
        task_completion(
            move || {
                read_bounded_in_place(
                    &file,
                    offset,
                    length,
                    require_full,
                    &observer,
                    #[cfg(windows)]
                    overlapped,
                )
            },
            Some(meter),
            None,
            Some(Arc::clone(&self.health)),
            &NATIVE_WORKERS,
        )
    }

    /// Submits owned positional reads while retaining this handle.
    #[must_use]
    pub fn read_batch_async(&self, reads: Vec<OwnedRead>) -> NativeCompletion<Vec<Bytes>> {
        NativeCompletion::<Vec<Bytes>>::submit(
            Arc::clone(&self.file),
            reads,
            Some(Arc::clone(&self.health)),
            obs::file_span!(DEBUG, "acyclic.runtime.read_batch"),
            #[cfg(windows)]
            self.overlapped,
        )
    }

    /// Submits owned writes while retaining this handle for later operations.
    #[must_use]
    pub fn write_all_batch_async(&self, writes: Vec<OwnedWrite>) -> NativeCompletion<()> {
        self.submit_unit(NativeUnitOperation::Write(writes), None)
            .expect("unmetered unit construction cannot fail")
    }

    /// Flushes in this file's operation order.
    pub fn sync_async(
        &self,
        durability: Durability,
        meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> io::Result<NativeCompletion<()>> {
        self.control_async(move |file, meter| sync_file(file, durability, meter), meter)
    }

    /// Writes a caller-accounted payload on this same ordered native driver.
    pub fn write_async<B: AsRef<[u8]> + Send + 'static, C: Send + 'static>(
        &self,
        offset: u64,
        bytes: B,
        meter: Arc<dyn NativeReadMeter>,
        custody: C,
    ) -> io::Result<NativeCompletion<()>> {
        let owned = (bytes, custody);
        meter.check()?;
        offset
            .checked_add(owned.0.as_ref().len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow"))?;
        #[cfg(windows)]
        if !self.overlapped {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.control_async(
            move |file, meter| {
                let meter = meter.expect("bounded write meter");
                let result = write_bounded_in_place(file, offset, owned.0.as_ref(), meter);
                drop(owned);
                result
            },
            Some(meter),
        )
    }

    pub(crate) fn submit_unit(
        &self,
        operation: NativeUnitOperation,
        meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> io::Result<NativeCompletion<()>> {
        if let Some(meter) = &meter {
            meter.check()?;
        }
        let allocation = meter
            .as_ref()
            .map(|meter| {
                ReadAllocation::new(meter, std::mem::size_of::<Mutex<CompletionState<()>>>())
            })
            .transpose()?;
        let state = Arc::new(Mutex::new(CompletionState {
            _allocation: allocation,
            ..CompletionState::default()
        }));
        let span = if meter.is_some() {
            obs::file_span!(DEBUG, "acyclic.runtime.bounded_unit")
        } else {
            match &operation {
                NativeUnitOperation::Write(writes) => {
                    obs::file_span!(DEBUG, "acyclic.runtime.write_batch")
                        .sized(writes.len(), || {
                            writes.iter().map(|write| write.bytes.len()).sum()
                        })
                }
                _ => obs::file_span!(DEBUG, "acyclic.runtime.control"),
            }
        };
        Ok(NativeCompletion::pending(
            NativeJob::Unit {
                file: Arc::clone(&self.file),
                operation,
                meter,
                span,
                #[cfg(windows)]
                overlapped: self.overlapped,
                state: Arc::clone(&state),
                completion: None,
                health: Some(Arc::clone(&self.health)),
            },
            state,
            &NATIVE_WORKERS,
        ))
    }
}
