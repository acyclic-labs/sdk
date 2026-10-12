use crate::*;

/// One file owned by the native I/O runtime.
pub struct NativeFile {
    pub(crate) file: Arc<NativeFilePayload>,
    pub(crate) health: Arc<FileHealth>,
    #[cfg(windows)]
    pub(crate) overlapped: bool,
}

/// Accounting supplied when importing a native handle.
pub enum NativeFileMeter<'a> {
    /// The caller releases the handle allocation.
    Borrowed(&'a dyn NativeReadMeter),
    /// The handle retains its original allocation claim.
    Owned(Arc<dyn NativeReadMeter>),
}

impl NativeFile {
    /// Imports a handle, returning it unchanged on admission failure.
    pub fn from_file(
        file: File,
        accounting: Option<NativeFileMeter<'_>>,
    ) -> Result<Self, (io::Error, File)> {
        let (meter, owner) = match &accounting {
            Some(NativeFileMeter::Borrowed(meter)) => (Some(*meter), None),
            Some(NativeFileMeter::Owned(meter)) => (Some(meter.as_ref()), Some(meter)),
            None => (None, None),
        };
        if let Some(meter) = meter {
            if let Err(error) = meter.check() {
                return Err((error, file));
            }
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            if FILE_HEALTH.get().is_none() {
                return Err((io::ErrorKind::NotConnected.into(), file));
            }
        }
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        if owner.is_some() {
            return Err((io::ErrorKind::Unsupported.into(), file));
        }
        let bytes = std::mem::size_of::<NativeFilePayload>();
        let allocation = match owner {
            Some(owner) => match ReadAllocation::new(owner, bytes) {
                Ok(allocation) => Some(allocation),
                Err(error) => return Err((error, file)),
            },
            None => {
                if let Some(meter) = meter
                    && let Err(error) = meter.allocate(bytes as u64)
                {
                    return Err((error, file));
                }
                None
            }
        };
        let payload = Arc::new(NativeFilePayload {
            file,
            _allocation: allocation,
        });
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let health = shared_file_health(&payload, meter);
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let health = meter
            .map_or(Ok(()), |meter| {
                meter.allocate(std::mem::size_of::<FileHealth>() as u64)
            })
            .map(|()| Arc::new(FileHealth::default()));
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
                    .expect("unpublished file import");
                if _allocation.is_none()
                    && let Some(meter) = meter
                {
                    meter.release(bytes as u64);
                }
                drop(_allocation);
                Err((error, file))
            }
        }
    }

    /// Logical storage charged when importing this handle.
    #[must_use]
    pub fn logical_owned_bytes(&self) -> u64 {
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let health = 0;
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let health = std::mem::size_of::<FileHealth>();
        (std::mem::size_of::<NativeFilePayload>() + health) as u64
    }

    /// Imports a Windows handle for overlapped I/O.
    ///
    /// # Safety
    /// The caller transfers sole I/O ownership of an idle FILE_FLAG_OVERLAPPED handle associated with no completion port.
    #[cfg(windows)]
    #[allow(unsafe_code, reason = "the caller supplies an idle overlapped handle")]
    pub unsafe fn from_overlapped_file_unchecked(
        file: File,
        meter: Option<NativeFileMeter<'_>>,
    ) -> Result<Self, (io::Error, File)> {
        let mut native = Self::from_file(file, meter)?;
        native.overlapped = true;
        Ok(native)
    }

    /// Changes file length in this handle's operation order.
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

    /// Runs a handle control in this file's operation order.
    pub fn control_async<F>(
        &self,
        operation: F,
        meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> io::Result<NativeCompletion<()>>
    where
        F: FnOnce(&File, Option<&dyn NativeReadMeter>) -> io::Result<()> + Send + 'static,
    {
        let file = Arc::clone(&self.file);
        let health = Arc::clone(&self.health);
        let observer = meter.clone();
        let span = if meter.is_some() {
            obs::file_span!(DEBUG, "acyclic.runtime.bounded_unit")
        } else {
            obs::file_span!(DEBUG, "acyclic.runtime.control")
        };
        task_completion(
            move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let result = span.in_scope(|| {
                        with_file_scope(Some(&health), || {
                            let result = operation(&file, observer.as_deref());
                            span.record_error(result.as_ref().err());
                            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                            if result.as_ref().err().is_some_and(is_uncertain_io_error) {
                                poison_file_health(&file, &health);
                            }
                            result
                        })
                    });
                    drop(file);
                    result
                }))
                .unwrap_or_else(|_| std::process::abort())
            },
            meter,
            None,
            Some(Arc::clone(&self.health)),
            &NATIVE_WORKERS,
        )
    }

    /// Borrows the held file for scalar metadata operations.
    #[must_use]
    pub fn as_file(&self) -> &File {
        &self.file
    }

    /// Reads storage whose AsMut exposes the same initialized slice without allocating.
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
                if destination.is_empty() {
                    return Ok((storage, 0));
                }
                #[cfg(windows)]
                let read = |file: &File| {
                    super::range::read_bounded_slice(
                        file,
                        offset,
                        destination,
                        require_full,
                        observer.as_ref(),
                    )
                };
                #[cfg(windows)]
                let filled = windows::with_overlapped(
                    &file,
                    windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_READ,
                    overlapped,
                    Some(observer.as_ref()),
                    read,
                )?;
                #[cfg(not(windows))]
                let filled = super::range::read_bounded_slice(
                    &file,
                    offset,
                    destination,
                    require_full,
                    observer.as_ref(),
                )?;
                Ok((storage, filled))
            },
            Some(meter),
            None,
            Some(Arc::clone(&self.health)),
            &NATIVE_WORKERS,
        )
    }

    /// Reads a bounded range and returns bytes before their allocation custody.
    pub fn read_bounded_async(
        &self,
        offset: u64,
        length: usize,
        meter: Arc<dyn NativeReadMeter>,
        require_full: bool,
    ) -> io::Result<NativeCompletion<(Bytes, Option<ReadAllocation>)>> {
        meter.check()?;
        meter.admit_bytes(length)?;
        if length == 0 {
            return Ok(NativeCompletion::ready(Ok((Bytes::new(), None))));
        }
        offset
            .checked_add(length as u64)
            .ok_or(io::ErrorKind::InvalidInput)?;
        #[cfg(windows)]
        if length > u32::MAX as usize || offset == u64::MAX {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let file = Arc::clone(&self.file);
        let observer = Arc::clone(&meter);
        #[cfg(windows)]
        let overlapped = self.overlapped;
        task_completion(
            move || {
                let read = |file: &File| {
                    let mut allocation = ReadAllocation::new(&observer, length)?;
                    let mut data = Vec::new();
                    data.try_reserve_exact(length)
                        .map_err(|error| observer.allocation_failed(error))?;
                    allocation.reconcile_capacity(data.capacity())?;
                    data.resize(length, 0);
                    let filled = super::range::read_bounded_slice(
                        file,
                        offset,
                        &mut data,
                        require_full,
                        observer.as_ref(),
                    )?;
                    if filled == 0 {
                        return Ok((Bytes::new(), None));
                    }
                    if filled != data.capacity() {
                        observer.allocate_refcount_header()?;
                    }
                    data.truncate(filled);
                    Ok((Bytes::from(data), Some(allocation)))
                };
                #[cfg(windows)]
                return windows::with_overlapped(
                    &file,
                    windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_READ,
                    overlapped,
                    Some(observer.as_ref()),
                    read,
                );
                #[cfg(not(windows))]
                read(&file)
            },
            Some(meter),
            None,
            Some(Arc::clone(&self.health)),
            &NATIVE_WORKERS,
        )
    }

    /// Submits positional reads while retaining this handle.
    pub fn read_batch_async(&self, reads: Vec<OwnedRead>) -> NativeCompletion<Vec<Bytes>> {
        NativeCompletion::submit(
            Arc::clone(&self.file),
            reads,
            Some(Arc::clone(&self.health)),
            obs::file_span!(DEBUG, "acyclic.runtime.read_batch"),
            #[cfg(windows)]
            self.overlapped,
        )
    }

    /// Submits owned writes while retaining this handle.
    pub fn write_all_batch_async(&self, writes: Vec<OwnedWrite>) -> NativeCompletion<()> {
        let state = Arc::new(Mutex::new(CompletionState::default()));
        let span = obs::file_span!(DEBUG, "acyclic.runtime.write_batch")
            .sized(writes.len(), || {
                writes.iter().map(|write| write.bytes.len()).sum()
            });
        NativeCompletion::pending(
            NativeJob::Write {
                file: Arc::clone(&self.file),
                writes,
                span,
                #[cfg(windows)]
                overlapped: self.overlapped,
                state: Arc::clone(&state),
                completion: None,
                health: Some(Arc::clone(&self.health)),
            },
            state,
            &NATIVE_WORKERS,
        )
    }

    /// Flushes in this file's operation order.
    pub fn sync_async(
        &self,
        durability: Durability,
        meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> io::Result<NativeCompletion<()>> {
        self.control_async(move |file, meter| sync_file(file, durability, meter), meter)
    }

    /// Writes a caller-accounted payload on this handle's ordered driver.
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
            .ok_or(io::ErrorKind::InvalidInput)?;
        #[cfg(windows)]
        if !self.overlapped {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.control_async(
            move |file, meter| {
                let result = write_bounded_in_place(
                    file,
                    offset,
                    owned.0.as_ref(),
                    meter.expect("bounded write meter"),
                );
                drop(owned);
                result
            },
            Some(meter),
        )
    }
}
