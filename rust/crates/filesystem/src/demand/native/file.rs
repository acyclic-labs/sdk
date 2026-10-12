#[derive(Clone)]
struct NativeDemandFile {
    provider: NativeDemandSource,
    source: SourceReference,
    relative: DemandPath,
    expected: SourceVersion,
    logical_bytes: u64,
    #[cfg(windows)]
    file: Arc<std::sync::RwLock<Arc<SourceFile>>>,
    #[cfg(not(windows))]
    file: Arc<SourceFile>,
    ownership: Arc<SourcePayloadOwnership>,
}

/// A source file held for reading. On Windows it gives way to whoever
#[cfg(windows)]
type SourceFile = crate::native_host::YieldingFile;
#[cfg(not(windows))]
type SourceFile = std::fs::File;

/// How long a read of a file under a root that is not local may take
const REMOTE_READ_DEADLINE: std::time::Duration = std::time::Duration::from_secs(20);
/// Most reads of files under roots that are not local that may be
const MAXIMUM_BLOCKED_REMOTE_READS: usize = 16;
static BLOCKED_REMOTE_READS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

impl NativeDemandFile {
    fn open(
        provider: NativeDemandSource,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        context: &mut CallbackWork,
    ) -> Result<Self, DemandError> {
        provider.check(source, context)?;
        provider.observe_parent(path, context)?;
        let (relative, allocation) = provider.relative(path, context)?;
        let ownership = SourcePayloadOwnership::new(context)?;
        ownership.retain(allocation, context)?;
        let controls = std::mem::size_of::<Self>() as u64
            + std::mem::size_of::<SourceFile>() as u64
            + 4 * std::mem::size_of::<usize>() as u64;
        context.claim(controls)?;
        ownership.retain(controls, context)?;
        #[cfg(windows)]
        let file = provider
            .inner
            .root
            .open_file_yielding_wide(&relative, context);
        #[cfg(not(windows))]
        let file = provider
            .inner
            .root
            .open_file(&relative, context)
            .map(cap_std::fs::File::into_std);
        let file = match file {
            Ok(file) => file,
            Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(DemandError::Absent);
            }
            Err(error) => return Err(error),
        };
        let opened = stat_held(&provider, &file, context)?;
        if !opened.file_type().is_file() {
            return Err(DemandError::NotRegularFile);
        }
        if version(&opened) != expected {
            return Err(DemandError::StaleVersion);
        }
        Ok(Self {
            provider,
            source,
            relative,
            ownership: Arc::new(ownership),
            expected,
            logical_bytes: opened.len(),
            #[cfg(windows)]
            file: Arc::new(std::sync::RwLock::new(Arc::new(file))),
            #[cfg(not(windows))]
            file: Arc::new(file),
        })
    }

    fn read(
        &self,
        offset: u64,
        length: u64,
        context: &mut CallbackWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
        #[cfg(windows)]
        let file = self
            .file
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        #[cfg(not(windows))]
        let file = self.file.clone();
        if length > 1024 * 1024 || offset.checked_add(length).is_none() {
            return Err(DemandError::InvalidRequest);
        }
        let result = self.read_once(&file, offset, length, context);
        #[cfg(windows)]
        if matches!(result, Err(DemandError::StaleVersion)) && file.was_yielded() {
            let reopened = Arc::new(self.reopen(context)?);
            let current = {
                let mut held = self
                    .file
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if Arc::ptr_eq(&held, &file) {
                    *held = reopened;
                }
                held.clone()
            };
            return self.read_once(&current, offset, length, context);
        }
        result
    }

    #[cfg(windows)]
    fn reopen(&self, context: &mut CallbackWork) -> Result<SourceFile, DemandError> {
        self.provider.check(self.source, context)?;
        let file = match self
            .provider
            .inner
            .root
            .open_file_yielding_wide(&self.relative, context)
        {
            Ok(file) => file,
            Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(DemandError::StaleVersion);
            }
            Err(error) => return Err(error),
        };
        let opened = stat_held(&self.provider, &file, context)?;
        if version(&opened) != self.expected {
            return Err(DemandError::StaleVersion);
        }
        Ok(file)
    }

    fn read_once(
        &self,
        file: &SourceFile,
        offset: u64,
        length: u64,
        context: &mut CallbackWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
        context.check()?;
        let capacity = usize::try_from(length.min(self.logical_bytes.saturating_sub(offset)))
            .map_err(|_| DemandError::InvalidRequest)?;
        let copying = WorkCounters {
            bytes_copied: capacity as u64,
            ..WorkCounters::UNCHARGED
        };
        context.admit(copying)?;
        let original = context.native_source_work();
        let (mut data, mut allocation, mut native_allocation) = if let Some(original) = &original {
            let owned = crate::native_host::native_buffer::<u8>(capacity, original)
                .map_err(crate::native_name::original_name_error)?;
            (owned.data, 0, Some(owned._allocation))
        } else {
            let mut allocation = context.claim(capacity as u64)?;
            let mut data = Vec::new();
            let reserved = data
                .try_reserve_exact(capacity)
                .map_err(DemandError::Allocation);
            let reconciled = context.reconcile_allocation(&mut allocation, data.capacity() as u64);
            if let Err(error) = reserved.and(reconciled) {
                drop(data);
                context.release(allocation)?;
                return Err(error);
            }
            (data, allocation, None)
        };
        let result = (|| {
            let copying = WorkCounters {
                bytes_copied: data.capacity() as u64,
                ..WorkCounters::UNCHARGED
            };
            context.admit(copying)?;
            data.resize(data.capacity(), 0);
            context.record(copying)?;
            let retained = data.capacity() as u64;
            if let Some(original) = original {
                let mut prepared = acyclic_stream::PreparedOwnedBytes::new(
                    std::mem::take(&mut data),
                    native_allocation.take(),
                    original.runtime_meter(),
                )
                .map_err(|failure| match failure.into_error() {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                        crate::native_name::original_name_error(original.runtime_error(error))
                    }
                    acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                        DemandError::InvalidRequest
                    }
                })?;
                let filled = self.fill(file, offset, prepared.writable(), context)?;
                self.prove_current(file, context)?;
                let bytes = prepared
                    .freeze(filled)
                    .map_err(|_| DemandError::InvalidRequest)?;
                Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, retained))
            } else {
                let filled = self.fill(file, offset, &mut data, context)?;
                self.prove_current(file, context)?;
                data.truncate(filled);
                Ok(crate::kernel::OwnedSourceChunk::claimed(
                    std::mem::take(&mut data).into(),
                    std::mem::take(&mut allocation),
                ))
            }
        })();
        drop(data);
        if allocation != 0 {
            context.release(allocation)?;
        }
        result
    }

    fn fill(
        &self,
        file: &SourceFile,
        offset: u64,
        bytes: &mut [u8],
        context: &mut CallbackWork,
    ) -> Result<usize, DemandError> {
        let mut filled = 0;
        while filled < bytes.len() {
            context.check()?;
            match read_at(file, offset + filled as u64, &mut bytes[filled..], context) {
                Ok(0) => break,
                Ok(count) => filled += count,
                Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        context.record(WorkCounters {
            output_bytes: filled as u64,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(filled)
    }
    fn prove_current(
        &self,
        file: &SourceFile,
        context: &mut CallbackWork,
    ) -> Result<(), DemandError> {
        let held = stat_held(&self.provider, file, context)?;
        #[cfg(unix)]
        let named = self.provider.inner.root.stat(&self.relative, context);
        #[cfg(windows)]
        let named = self.provider.inner.root.stat_wide(&self.relative, context);
        if version(&held) != self.expected
            || named.as_ref().map(version).ok() != Some(self.expected)
        {
            self.provider.check(self.source, context)?;
            return Err(DemandError::StaleVersion);
        }
        self.provider.check(self.source, context)
    }
}

impl DemandFile for NativeDemandFile {
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
    fn read_range(
        &self,
        offset: u64,
        length: u64,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
        let mut context = match accounting.native_work() {
            Some(original) => CallbackWork::Original(original.clone()),
            None => CallbackWork::Ordinary(DemandWork::new(
                accounting.remaining().map_err(DemandError::from)?,
                match &*accounting {
                    crate::kernel::KernelAccounting::Standalone { cancellation, .. }
                    | crate::kernel::KernelAccounting::BorrowedStandalone {
                        cancellation, ..
                    }
                    | crate::kernel::KernelAccounting::NativeOriginal { cancellation, .. } => {
                        cancellation
                    }
                },
            )),
        };
        if self.provider.inner.inline {
            let result = self.read(offset, length, &mut context);
            return finish_context(result, context, accounting);
        }
        if BLOCKED_REMOTE_READS.fetch_add(1, Ordering::AcqRel) >= MAXIMUM_BLOCKED_REMOTE_READS {
            BLOCKED_REMOTE_READS.fetch_sub(1, Ordering::AcqRel);
            return Err(DemandError::SourceUnavailable);
        }
        let file = self.clone();
        let (answer, answered) = std::sync::mpsc::sync_channel(1);
        let spawned = std::thread::Builder::new()
            .name("acyclic-remote-read".to_owned())
            .spawn(move || {
                let result = file.read(offset, length, &mut context);
                BLOCKED_REMOTE_READS.fetch_sub(1, Ordering::AcqRel);
                let _ = answer.send((result, context));
            });
        if spawned.is_err() {
            BLOCKED_REMOTE_READS.fetch_sub(1, Ordering::AcqRel);
            return Err(DemandError::SourceUnavailable);
        }
        let (result, context) = answered
            .recv_timeout(REMOTE_READ_DEADLINE)
            .map_err(|_| DemandError::SourceUnavailable)?;
        finish_context(result, context, accounting)
    }
}

#[cfg(unix)]
fn read_at(
    file: &std::fs::File,
    offset: u64,
    destination: &mut [u8],
    context: &mut CallbackWork,
) -> Result<usize, DemandError> {
    context.admit(WorkCounters {
        source_bytes_read: destination.len() as u64,
        ..WorkCounters::UNCHARGED
    })?;
    context.started_io(WorkCounters {
        backend_read_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let count = std::os::unix::fs::FileExt::read_at(file, destination, offset)?;
    context.record(WorkCounters {
        source_bytes_read: count as u64,
        ..WorkCounters::UNCHARGED
    })?;
    Ok(count)
}

#[cfg(windows)]
fn read_at(
    file: &SourceFile,
    offset: u64,
    destination: &mut [u8],
    context: &mut CallbackWork,
) -> Result<usize, DemandError> {
    file.read_at(offset, destination, context)
}

fn stat_held(
    provider: &NativeDemandSource,
    file: &SourceFile,
    context: &mut CallbackWork,
) -> Result<HostStat, DemandError> {
    #[cfg(windows)]
    {
        file.with(|file| provider.inner.root.stat_file(file, context))
    }
    #[cfg(not(windows))]
    {
        provider.inner.root.stat_file(file, context)
    }
}
