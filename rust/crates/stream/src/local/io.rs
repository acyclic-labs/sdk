use super::*;

pub(super) fn fund_task<F>(value: F, meter: Option<&Arc<dyn NativeReadMeter>>) -> std::io::Result<MemoryOutput<F>> {
    let allocation = meter.map(|meter| ReadAllocation::new(
        meter, std::mem::size_of::<MemoryOutput<F>>(),
    )).transpose()?;
    Ok(MemoryOutput { value, allocation })
}

impl<F: Future> Future for MemoryOutput<F> {
    type Output = F::Output;
    fn poll(self: std::pin::Pin<&mut Self>, context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.project().value.poll(context)
    }
}

/// Request-owned native allowance, admitted while the mutation can still decline without editing semantic state.
pub(super) struct AdmittedFrameWrite {
    pub(super) meter: Option<Arc<dyn NativeReadMeter>>,
    pub(super) operations: Option<NativeOperationReservation<Arc<dyn NativeReadMeter>>>,
    pub(super) bytes: NativeByteReservation<Arc<dyn NativeReadMeter>>,
}

impl AdmittedFrameWrite {
    pub(super) fn new(meter: &Arc<dyn NativeReadMeter>, length: usize) -> std::io::Result<Self> {
        let operations = NativeOperationReservation::new(Arc::clone(meter), NativeOperationKind::Write, 1)?;
        let bytes = NativeByteReservation::new(Arc::clone(meter), NativeOperationKind::Write, length)?;
        Ok(Self { meter: Some(Arc::clone(meter)), operations: Some(operations), bytes })
    }

    pub(super) fn close_plan(mut self) -> Self { self.meter = None; self }

    pub(super) fn write_all(&mut self, file: &mut impl Write, mut source: &[u8])
        -> (Result<(), LocalStreamError>, std::io::Result<()>) {
        let mut observed = Ok(());
        let physical = (|| {
            while !source.is_empty() {
                let mut operation = match self.operations.take() {
                    Some(operation) => operation,
                    None => {
                        let meter = self.meter.as_ref().ok_or(LocalStreamError::IncompleteFrame)?;
                        NativeOperationReservation::new(Arc::clone(meter), NativeOperationKind::Write, 1)?
                    }
                };
                let (written, recorded) = operation.attempt(|| file.write(source));
                let bytes = self.bytes.record_partial(written.as_ref().copied().unwrap_or(0));
                keep_observation(&mut observed, recorded);
                keep_observation(&mut observed, bytes);
                match written {
                    Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()),
                    Ok(count) => source = &source[count..],
                    Err(error) if self.meter.is_some() && error.kind() == std::io::ErrorKind::Interrupted => {},
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(())
        })();
        keep_observation(&mut observed, self.bytes.record(0));
        (physical, observed)
    }
}

pub(super) fn attempt_control<T>(
    admission: &mut Option<NativeOperationReservation<Arc<dyn NativeReadMeter>>>,
    operation: impl FnOnce() -> T,
) -> (T, std::io::Result<()>) {
    match admission {
        Some(admission) => admission.attempt(operation),
        None => (operation(), Ok(())),
    }
}

pub(super) fn keep_observation(first: &mut std::io::Result<()>, next: std::io::Result<()>) {
    if first.is_ok() { *first = next; }
}

pub(super) fn joined_path(
    root: &Path,
    leaf: &str,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> std::io::Result<MemoryOutput<PathBuf>> {
    let root_bytes = root.as_os_str().as_encoded_bytes().len();
    let capacity = root_bytes.checked_add(leaf.len()).and_then(|length| length.checked_add(1))
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    if let Some(meter) = meter { meter.admit_codec(0, capacity as u64, 0)?; }
    let mut allocation = meter.map(|meter| ReadAllocation::new(meter, capacity)).transpose()?;
    let mut path = PathBuf::new();
    if let Some(meter) = meter {
        path.try_reserve_exact(capacity).map_err(|error| meter.allocation_failed(error))?;
    } else {
        path.reserve_exact(capacity);
    }
    if let Some(allocation) = &mut allocation { allocation.reconcile_capacity(path.capacity())?; }
    path.push(root);
    let observed = meter.map_or(Ok(()), |meter| meter.record_codec(0, root_bytes as u64, 0));
    let retained = allocation.as_mut().map_or(Ok(()), |allocation| {
        allocation.reconcile_capacity(path.capacity())
    });
    observed?;
    retained?;
    if let Some(meter) = meter { meter.check()?; }
    if let Some(meter) = meter { meter.admit_codec(0, leaf.len() as u64, 0)?; }
    path.push(leaf);
    let observed = meter.map_or(Ok(()), |meter| meter.record_codec(0, leaf.len() as u64, 0));
    let capacity = allocation.as_mut().map_or(Ok(()), |allocation| {
        allocation.reconcile_capacity(path.capacity())
    });
    observed?;
    capacity?;
    if let Some(meter) = meter { meter.check()?; }
    Ok(MemoryOutput { value: path, allocation: allocation })
}

/// The temporary file's real close is accepted before acquiring the handle.
pub(super) struct SnapshotFile {
    pub(super) file: Option<File>,
    pub(super) meter: Option<Arc<dyn NativeReadMeter>>,
    pub(super) cleanup: Option<u64>,
}

impl SnapshotFile {
    pub(super) fn create(
        path: &Path,
        meter: Option<&Arc<dyn NativeReadMeter>>,
        controls: &mut Option<NativeOperationReservation<Arc<dyn NativeReadMeter>>>,
        observed: &mut std::io::Result<()>,
        started: &mut bool,
    ) -> Result<Self, LocalStreamError> {
        let cleanup = meter.map(|meter| meter.reserve_cleanup_write()).transpose()?;
        let mut owned = Self { file: None, meter: meter.cloned(), cleanup };
        *started = true;
        let (file, recorded) = attempt_control(controls, || File::create(path));
        keep_observation(observed, recorded);
        owned.file = Some(file?);
        Ok(owned)
    }

    pub(super) fn close(mut self) -> std::io::Result<()> {
        let observed = match (&self.meter, self.cleanup.take()) {
            (Some(meter), Some(cleanup)) => meter.record_cleanup_write(cleanup),
            _ => Ok(()),
        };
        drop(self.file.take());
        observed
    }
}

impl Drop for SnapshotFile {
    fn drop(&mut self) {
        if let (Some(meter), Some(cleanup)) = (&self.meter, self.cleanup.take()) {
            if self.file.is_some() {
                let _ = meter.record_cleanup_write(cleanup);
            } else {
                meter.cancel_cleanup_write(cleanup);
            }
        }
        drop(self.file.take());
    }
}

pub(super) fn sync_file(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, native_durability(durability), None)
}

pub(super) fn sync_file_data(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_data(file, native_durability(durability))
}

pub(super) fn native_durability(durability: LocalDurability) -> acyclic_native_runtime::Durability {
    match durability {
        LocalDurability::FullFlush => acyclic_native_runtime::Durability::Full,
        LocalDurability::Barrier => acyclic_native_runtime::Durability::Barrier,
    }
}

pub(super) fn sync_directory(path: &Path, durability: LocalDurability) -> Result<(), LocalStreamError> {
    acyclic_native_runtime::sync_parent(path, native_durability(durability))?;
    Ok(())
}
