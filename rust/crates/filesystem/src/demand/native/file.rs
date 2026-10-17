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
    _ownership: Arc<SourcePayloadOwnership>,
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
static REMOTE_READERS: std::sync::LazyLock<Arc<remote::RemoteReaders<RemoteDemandRead>>> =
    std::sync::LazyLock::new(|| remote::RemoteReaders::new(MAXIMUM_BLOCKED_REMOTE_READS));

#[cfg(not(test))]
type RemoteAnswer = (
    Result<crate::kernel::OwnedSourceChunk, DemandError>,
    CallbackWork,
);

#[cfg(test)]
type RemoteAnswer = (
    Result<crate::kernel::OwnedSourceChunk, DemandError>,
    CallbackWork,
    Vec<(&'static str, u64, u64)>,
);

/// One held-file read for a remote reader, answered to its waiting callback.
struct RemoteDemandRead {
    file: NativeDemandFile,
    offset: u64,
    length: u64,
    context: CallbackWork,
    answer: std::sync::mpsc::SyncSender<RemoteAnswer>,
}

impl remote::RemoteRead for RemoteDemandRead {
    type Delivery = (std::sync::mpsc::SyncSender<RemoteAnswer>, RemoteAnswer);
    fn read(mut self) -> Self::Delivery {
        #[cfg(not(test))]
        let result = self.file.read(self.offset, self.length, &mut self.context);
        #[cfg(test)]
        let (result, trace) = demand_read_work::capture(|| {
            self.file.read(self.offset, self.length, &mut self.context)
        });
        #[cfg(not(test))]
        let answer = (result, self.context);
        #[cfg(test)]
        let answer = (result, self.context, trace);
        (self.answer, answer)
    }
}

impl remote::RemoteDelivery for (std::sync::mpsc::SyncSender<RemoteAnswer>, RemoteAnswer) {
    fn deliver(self) {
        // A caller past its deadline has gone; its answer drops here.
        let _ = self.0.send(self.1);
    }
}

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
        let file = provider.inner.root.open_file_yielding(&relative, context);
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
        if opened.file_type() != crate::demand::SourceNodeKind::RegularFile {
            return Err(DemandError::NotRegularFile);
        }
        if version(&opened) != expected {
            return Err(DemandError::StaleVersion);
        }
        Ok(Self {
            provider,
            source,
            relative,
            _ownership: Arc::new(ownership),
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
            .open_file_yielding(&self.relative, context)
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
        #[cfg(test)]
        if demand_read_work::take_extra() {
            let (extra, allocation) = crate::native_host::buffer::<u8, _>(17, context)?;
            let result = self.read_once(file, offset, length, context);
            drop(extra);
            context.release(allocation)?;
            return result;
        }
        context.check()?;
        let capacity = usize::try_from(length.min(self.logical_bytes.saturating_sub(offset)))
            .map_err(|_| DemandError::InvalidRequest)?;
        context.admit(WorkCounters {
            output_bytes: capacity as u64,
            ..WorkCounters::UNCHARGED
        })?;
        if let Some(original) = context.native_source_work() {
            let meter = original.runtime_meter();
            let native =
                |error| crate::native_name::original_name_error(original.runtime_error(error));
            let header = acyclic_stream::OwnedBytes::reserve_vec_guard::<
                Option<crate::NativeAllocation>,
            >(&meter)
            .map_err(native)?;
            let mut data = Charged::<Vec<u8>>::buffer(capacity, &original)?;
            #[cfg(test)]
            demand_read_work::storage(&mut data);
            let retained = data.capacity() as u64;
            self.fill(file, offset, &mut data, capacity, context)?;
            self.prove_current(file, context)?;
            context.record(WorkCounters {
                output_bytes: data.len() as u64,
                ..WorkCounters::UNCHARGED
            })?;
            let bytes = data.into_owned_bytes(header).map_err(|error| match error {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => native(error),
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                    DemandError::InvalidRequest
                }
            })?;
            return Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, retained));
        }
        let (mut data, allocation) = crate::native_host::buffer::<u8, _>(capacity, context)?;
        #[cfg(test)]
        demand_read_work::storage(&mut data);
        let result = self
            .fill(file, offset, &mut data, capacity, context)
            .and_then(|()| self.prove_current(file, context))
            .and_then(|()| {
                context.record(WorkCounters {
                    output_bytes: data.len() as u64,
                    ..WorkCounters::UNCHARGED
                })
            });
        if let Err(error) = result {
            drop(data);
            context.release(allocation)?;
            return Err(error);
        }
        Ok(crate::kernel::OwnedSourceChunk::claimed(
            data.into(),
            allocation,
        ))
    }

    /// Appends the file's bytes at `offset` to `data` until it holds
    /// `capacity` or the file ends, reading into uninitialized capacity.
    fn fill(
        &self,
        file: &SourceFile,
        offset: u64,
        data: &mut Vec<u8>,
        capacity: usize,
        context: &mut CallbackWork,
    ) -> Result<(), DemandError> {
        while data.len() < capacity {
            context.check()?;
            let position = offset
                .checked_add(data.len() as u64)
                .ok_or(crate::WorkError::Overflow)?;
            let remaining = capacity - data.len();
            match read_spare_at(file, position, data, remaining, context) {
                Ok(0) => break,
                Ok(_) => {}
                Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    fn prove_current(
        &self,
        file: &SourceFile,
        context: &mut CallbackWork,
    ) -> Result<(), DemandError> {
        #[cfg(test)]
        demand_read_work::record("held-stat", 0, 0);
        let held = stat_held(&self.provider, file, context)?;
        #[cfg(test)]
        demand_read_work::record("path-stat", 0, 0);
        #[cfg(unix)]
        let named = self.provider.inner.root.stat(&self.relative, context);
        #[cfg(windows)]
        let named = self.provider.inner.root.stat(&self.relative, context);
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
        let mut context = callback_work(accounting)?;
        if self.provider.inner.inline {
            let result = self.read(offset, length, &mut context);
            return finish_context(result, context, accounting);
        }
        let (answer, answered) = std::sync::mpsc::sync_channel(1);
        let read = RemoteDemandRead {
            file: self.clone(),
            offset,
            length,
            context,
            answer,
        };
        if self.provider.inner.remote.submit(read).is_err() {
            return Err(DemandError::SourceUnavailable);
        }
        let answer = answered
            .recv_timeout(REMOTE_READ_DEADLINE)
            .map_err(|_| DemandError::SourceUnavailable)?;
        #[cfg(not(test))]
        let (result, context) = answer;
        #[cfg(test)]
        let (result, context, trace) = answer;
        #[cfg(test)]
        demand_read_work::replay(trace);
        finish_context(result, context, accounting)
    }
}

/// One positional read appended to `data`'s uninitialized capacity.
#[cfg(unix)]
fn read_spare_at(
    file: &std::fs::File,
    offset: u64,
    data: &mut Vec<u8>,
    length: usize,
    context: &mut CallbackWork,
) -> Result<usize, DemandError> {
    #[cfg(test)]
    demand_read_work::record("read", offset, length);
    #[cfg(test)]
    demand_read_work::injected_error()?;
    context.admit(WorkCounters {
        source_bytes_read: length as u64,
        ..WorkCounters::UNCHARGED
    })?;
    context.started_io(WorkCounters {
        backend_read_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let count = crate::native_host::read_spare_at(file, offset, data, length)?;
    context.record(WorkCounters {
        source_bytes_read: count as u64,
        ..WorkCounters::UNCHARGED
    })?;
    Ok(count)
}

/// One positional read appended to `data`'s uninitialized capacity.
#[cfg(windows)]
fn read_spare_at(
    file: &SourceFile,
    offset: u64,
    data: &mut Vec<u8>,
    length: usize,
    context: &mut CallbackWork,
) -> Result<usize, DemandError> {
    #[cfg(test)]
    demand_read_work::record("read", offset, length);
    #[cfg(test)]
    demand_read_work::injected_error()?;
    file.read_spare_at(offset, data, length, context)
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
