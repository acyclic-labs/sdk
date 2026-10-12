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
    /// The caller retains responsibility for releasing the handle allocation.
    Borrowed(&'a dyn NativeReadMeter),
    /// The handle retains and releases its original allocation claim.
    Owned(Arc<dyn NativeReadMeter>),
}

impl NativeFile {
    /// Imports an authorized handle, returning it unchanged when admission fails.
    pub fn from_file(
        file: File, meter: Option<NativeFileMeter<'_>>,
    ) -> Result<Self, (io::Error, File)> {
        let Some(meter) = meter else {
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            let health = match shared_file_health(&file, None) {
                Ok(health) => health,
                Err(error) => return Err((error, file)),
            };
            #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
            let health = Arc::new(FileHealth::default());
            return Ok(Self {
                file: Arc::new(NativeFilePayload::unmetered(file)), health,
                #[cfg(windows)]
                overlapped: false,
            });
        };
        let (meter, owner) = match &meter {
            NativeFileMeter::Borrowed(meter) => (*meter, None),
            NativeFileMeter::Owned(meter) => (meter.as_ref(), Some(meter)),
        };
        if let Err(error) = meter.check() { return Err((error, file)); }
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        if FILE_HEALTH.get().is_none() {
            return Err((io::ErrorKind::NotConnected.into(), file));
        }
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        if owner.is_some() { return Err((io::ErrorKind::Unsupported.into(), file)); }
        let file_bytes = std::mem::size_of::<NativeFilePayload>();
        let allocation = if let Some(owner) = owner {
            match ReadAllocation::new(owner, file_bytes) {
                Ok(allocation) => Some(allocation),
                Err(error) => return Err((error, file)),
            }
        } else {
            if let Err(error) = meter.allocate(file_bytes as u64) { return Err((error, file)); }
            None
        };
        let file = Arc::new(NativeFilePayload { file, _allocation: allocation });
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let health = shared_file_health(&file, Some(meter));
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let health = meter.allocate(std::mem::size_of::<FileHealth>() as u64)
            .map(|()| Arc::new(FileHealth::default()));
        let health = match health {
            Ok(health) => health,
            Err(error) => {
                let payload = Arc::try_unwrap(file).ok().expect("unpublished native file import");
                let NativeFilePayload { file, _allocation } = payload;
                if _allocation.is_none() { meter.release(file_bytes as u64); }
                drop(_allocation);
                return Err((error, file));
            }
        };
        Ok(Self {
            file, health,
            #[cfg(windows)]
            overlapped: false,
        })
    }

    /// Logical handle storage owned by this capability, excluding the shared runtime health/sequencing control root.
    #[must_use]
    pub fn logical_owned_bytes(&self) -> u64 {
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let health = 0;
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let health = std::mem::size_of::<FileHealth>();
        (std::mem::size_of::<NativeFilePayload>() + health) as u64
    }

    /// Transfers a Windows handle already opened for overlapped I/O.
    ///
    /// # Safety
    /// The handle must have been opened with `FILE_FLAG_OVERLAPPED`, must not already be associated with an I/O completion port, and must have no concurrent independent I/O outside this `NativeFile`. The caller must transfer the sole I/O ownership of the handle to this function.
    #[cfg(windows)]
    #[allow(
        unsafe_code,
        reason = "the caller must uphold the overlapped-handle ownership contract"
    )]
    pub unsafe fn from_overlapped_file_unchecked(
        file: File, meter: Option<NativeFileMeter<'_>>,
    ) -> Result<Self, (io::Error, File)> {
        let mut native = Self::from_file(file, meter)?;
        native.overlapped = true;
        Ok(native)
    }

    /// Changes logical file length in this file's operation order.
    pub fn set_len_async(&self, size: u64, meter: Option<Arc<dyn NativeReadMeter>>) -> io::Result<NativeCompletion<()>> {
        self.submit_unit(NativeUnitOperation::SetLen(size), meter)
    }

    /// Runs a platform-specific handle control in this file's operation order.
    pub fn control_async<F>(
        &self, operation: F, meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> io::Result<NativeCompletion<()>>
    where F: FnOnce(&File, Option<&dyn NativeReadMeter>) -> io::Result<()> + Send + 'static {
        if let Some(meter) = &meter { meter.check()?; }
        let allocation = meter.as_ref().map(|meter| ReadAllocation::new(
            meter, std::mem::size_of::<F>(),
        )).transpose()?;
        self.submit_unit(NativeUnitOperation::Control {
            operation: Box::new(operation), _allocation: allocation,
        }, meter)
    }

    /// Borrows the already-held file for measured scalar metadata operations.
    #[must_use]
    pub fn as_file(&self) -> &File {
        &self.file
    }

    /// Reads caller-owned storage; `AsMut` must expose the same initialized slice without allocating.
    pub fn read_prepared_bounded_async<B>(
        &self, offset: u64, mut storage: B, length: usize, require_full: bool,
        meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<NativeCompletion<(B, usize)>>
    where B: AsMut<[u8]> + Send + 'static {
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
        task_completion(move || {
            let destination = storage.as_mut().get_mut(..length).ok_or(io::ErrorKind::InvalidInput)?;
            read_prepared_in_place(&file, offset, destination, require_full, observer.as_ref(),
                #[cfg(windows)]
                overlapped,
            ).map(|filled| (storage, filled))
        }, Some(meter), None, Some(Arc::clone(&self.health)), &NATIVE_WORKERS)
    }

    /// Reads a bounded range through this file's ordered worker queue.
    pub fn read_bounded_async(
        &self, offset: u64, length: usize, meter: Arc<dyn NativeReadMeter>,
        require_full: bool,
    ) -> io::Result<NativeCompletion<NativeReadResult>> {
        meter.check()?;
        meter.admit_bytes(length)?;
        if length == 0 {
            return Ok(NativeCompletion::ready(Ok(NativeReadResult {
                bytes: Bytes::new(), buffer_capacity: 0, _allocation: None,
            })));
        }
        offset.checked_add(length as u64)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        #[cfg(windows)]
        if length > u32::MAX as usize || offset == u64::MAX {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        let file = Arc::clone(&self.file);
        let observer = Arc::clone(&meter);
        #[cfg(windows)]
        let overlapped = self.overlapped;
        task_completion(move || read_bounded_in_place(&file, offset, length, require_full, &observer,
            #[cfg(windows)]
            overlapped,
        ), Some(meter), None, Some(Arc::clone(&self.health)), &NATIVE_WORKERS)
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
    pub fn sync_async(&self, durability: Durability, meter: Option<Arc<dyn NativeReadMeter>>) -> io::Result<NativeCompletion<()>> {
        self.submit_unit(NativeUnitOperation::Sync(durability), meter)
    }

    /// Writes a caller-accounted payload on this same ordered native driver.
    pub fn write_async<B: AsRef<[u8]> + Send + 'static, C: Send + 'static>(
        &self, offset: u64, bytes: B,
        meter: Arc<dyn NativeReadMeter>, custody: C,
    ) -> io::Result<NativeCompletion<()>> {
        let owned = (bytes, custody);
        meter.check()?;
        offset.checked_add(owned.0.as_ref().len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow"))?;
        #[cfg(windows)]
        if !self.overlapped { return Err(io::ErrorKind::InvalidInput.into()); }
        self.control_async(move |file, meter| {
            let meter = meter.expect("bounded write meter");
            let result = write_bounded_in_place(file, offset, owned.0.as_ref(), meter);
            drop(owned);
            result
        }, Some(meter))
    }

    pub(crate) fn submit_unit(
        &self, operation: NativeUnitOperation, meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> io::Result<NativeCompletion<()>> {
        if let Some(meter) = &meter { meter.check()?; }
        let allocation = meter.as_ref().map(|meter| ReadAllocation::new(
            meter, std::mem::size_of::<Mutex<CompletionState<()>>>(),
        )).transpose()?;
        let state = Arc::new(Mutex::new(CompletionState { _allocation: allocation, ..CompletionState::default() }));
        let span = if meter.is_some() { obs::file_span!(DEBUG, "acyclic.runtime.bounded_unit") }
            else { match &operation {
                NativeUnitOperation::Write(writes) => obs::file_span!(DEBUG, "acyclic.runtime.write_batch")
                    .sized(writes.len(), || writes.iter().map(|write| write.bytes.len()).sum()),
                NativeUnitOperation::SetLen(_) => obs::file_span!(DEBUG, "acyclic.runtime.set_len"),
                NativeUnitOperation::Sync(_) => obs::file_span!(DEBUG, "acyclic.runtime.sync"),
                _ => obs::file_span!(DEBUG, "acyclic.runtime.control"),
            } };
        Ok(NativeCompletion::pending(NativeJob::Unit {
            file: Arc::clone(&self.file), operation, meter, span,
            #[cfg(windows)]
            overlapped: self.overlapped,
            state: Arc::clone(&state), completion: None,
            health: Some(Arc::clone(&self.health)),
        }, state, &NATIVE_WORKERS))
    }

}
