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
/// Credits for remote filesystem reads whose OS calls may remain blocked.
const MAXIMUM_BLOCKED_REMOTE_READS: usize = 16;
static REMOTE_READ_CREDITS: RemoteReadCredits =
    RemoteReadCredits::new(MAXIMUM_BLOCKED_REMOTE_READS, || {
        REMOTE_READ_CREDITS.wake_cancelled()
    });

struct RemoteReadCredits {
    active: std::sync::Mutex<usize>,
    changed: std::sync::Condvar,
    maximum: usize,
    notify: fn(),
}

impl RemoteReadCredits {
    const fn new(maximum: usize, notify: fn()) -> Self {
        Self {
            active: std::sync::Mutex::new(0),
            changed: std::sync::Condvar::new(),
            maximum,
            notify,
        }
    }

    fn wake_cancelled(&self) {
        // Synchronize with the predicate check before Condvar::wait releases
        // this mutex. Cancellation never needs the original ledger here.
        let _active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.changed.notify_all();
    }

    fn acquire(&'static self, context: &mut CallbackWork) -> Result<RemoteReadCredit, DemandError> {
        context.check()?;
        {
            let mut active = self
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if *active < self.maximum {
                *active += 1;
                return Ok(RemoteReadCredit { pool: self });
            }
        }
        let tokens = context.with_original_leaf(|work| work.cancellation_tokens());
        let mut waiting = RemoteReadWait {
            context,
            waits: [None, None],
            reserved: 0,
        };
        for (index, token) in tokens.iter().enumerate() {
            if index != 0 && token.same_state(&tokens[0]) {
                continue;
            }
            waiting.prepare(index, token, self.notify)?;
        }
        let result = loop {
            waiting.context.check()?;
            let mut active = self
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if tokens.iter().any(CancellationToken::is_cancelled) {
                break Err(DemandError::Cancelled);
            }
            if *active < self.maximum {
                *active += 1;
                break Ok(RemoteReadCredit { pool: self });
            }
            // No original-ledger loan spans this wait. Credit retirement and
            // both original/authority and invocation cancellation signal it.
            drop(
                self.changed
                    .wait(active)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        };
        waiting.finish()?;
        result
    }
}

struct RemoteReadWait<'a> {
    context: &'a mut CallbackWork,
    waits: [Option<crate::cancellation::Cancelled<'static>>; 2],
    reserved: u64,
}

impl RemoteReadWait<'_> {
    fn prepare(
        &mut self,
        index: usize,
        token: &CancellationToken,
        notify: fn(),
    ) -> Result<(), DemandError> {
        let wait = match self.context {
            CallbackWork::Original(work) => token.prepare_cancelled_with_native_work(work),
            CallbackWork::Ordinary(work) => work.prepare_cancelled(token),
        }
        .map_err(remote_read_wait_error)?;
        let cleanup = wait.cleanup_items();
        self.waits[index] = Some(wait);
        if let CallbackWork::Ordinary(work) = self.context {
            let mut budget = work.budget;
            budget.items_examined = budget
                .items_examined
                .checked_sub(cleanup)
                .ok_or(crate::WorkError::Overflow)?;
            work.work.verify(budget)?;
            work.budget = budget;
            self.reserved += cleanup;
        }
        self.waits[index]
            .as_mut()
            .expect("prepared cancellation")
            .notify(notify);
        Ok(())
    }

    fn finish(&mut self) -> Result<(), DemandError> {
        let mut waits = std::mem::take(&mut self.waits);
        for wait in waits.iter_mut().flatten() {
            wait.retire_waker();
        }
        if let CallbackWork::Ordinary(work) = self.context {
            work.budget.items_examined = work
                .budget
                .items_examined
                .checked_add(std::mem::take(&mut self.reserved))
                .ok_or(crate::WorkError::Overflow)?;
            let mut result = Ok(());
            for wait in waits.into_iter().flatten() {
                result = result.and(work.finish_cancelled(wait).map_err(remote_read_wait_error));
            }
            result
        } else {
            Ok(())
        }
    }
}

impl Drop for RemoteReadWait<'_> {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

fn remote_read_wait_error(error: crate::cancellation::CancellationWaitError) -> DemandError {
    match error {
        crate::cancellation::CancellationWaitError::Accounting(error) => error.into(),
        crate::cancellation::CancellationWaitError::Work(error) => error.into(),
        crate::cancellation::CancellationWaitError::Native(error) => {
            crate::native_name::original_name_error(error)
        }
    }
}

struct RemoteReadCredit {
    pool: &'static RemoteReadCredits,
}

impl RemoteReadCredit {
    fn acquire(context: &mut CallbackWork) -> Result<Self, DemandError> {
        REMOTE_READ_CREDITS.acquire(context)
    }
}

impl Drop for RemoteReadCredit {
    fn drop(&mut self) {
        let mut active = self
            .pool
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active -= 1;
        self.pool.changed.notify_one();
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
        let file = provider
            .inner
            .root
            .held(context)
            .and_then(|root| root.open_file_yielding(&relative, context));
        #[cfg(not(windows))]
        let file = provider
            .inner
            .root
            .held(context)
            .and_then(|root| root.open_file(&relative, context))
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
            .held(context)
            .and_then(|root| root.open_file_yielding(&self.relative, context))
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
        context.admit(WorkCounters {
            output_bytes: capacity as u64,
            ..copying
        })?;
        let original = context.native_source_work();
        let (mut data, mut allocation, mut native_allocation) = if let Some(original) = &original {
            let (data, allocation) = crate::native_host::buffer::<u8, _>(capacity, &mut &*original)
                .map_err(crate::native_name::original_name_error)?;
            (data, 0, Some(allocation))
        } else {
            let (data, allocation) = crate::native_host::buffer::<u8, _>(capacity, context)?;
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
                    &original.runtime_meter(),
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
                context.record(WorkCounters {
                    output_bytes: filled as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                let bytes = prepared
                    .freeze(filled)
                    .map_err(|_| DemandError::InvalidRequest)?;
                Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, retained))
            } else {
                let filled = self.fill(file, offset, &mut data, context)?;
                self.prove_current(file, context)?;
                context.record(WorkCounters {
                    output_bytes: filled as u64,
                    ..WorkCounters::UNCHARGED
                })?;
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
            let position = offset
                .checked_add(filled as u64)
                .ok_or(crate::WorkError::Overflow)?;
            let output = bytes.get_mut(filled..).ok_or(DemandError::InvalidRequest)?;
            match read_at(file, position, output, context) {
                Ok(0) => break,
                Ok(count) => {
                    filled = filled
                        .checked_add(count)
                        .ok_or(crate::WorkError::Overflow)?
                }
                Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(filled)
    }
    fn prove_current(
        &self,
        file: &SourceFile,
        context: &mut CallbackWork,
    ) -> Result<(), DemandError> {
        let held = stat_held(&self.provider, file, context)?;
        #[cfg(unix)]
        let named = self
            .provider
            .inner
            .root
            .held(context)
            .and_then(|root| root.stat(&self.relative, context));
        #[cfg(windows)]
        let named = self
            .provider
            .inner
            .root
            .held(context)
            .and_then(|root| root.stat(&self.relative, context));
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
        let credit = RemoteReadCredit::acquire(&mut context)?;
        let file = self.clone();
        let (answer, answered) = std::sync::mpsc::sync_channel(1);
        let spawned = std::thread::Builder::new()
            .name("acyclic-remote-read".to_owned())
            .spawn(move || {
                let result = file.read(offset, length, &mut context);
                drop(credit);
                let _ = answer.send((result, context));
            });
        if spawned.is_err() {
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
        file.with(|file| {
            provider
                .inner
                .root
                .held(context)
                .and_then(|root| root.stat_file(file, context))
        })
    }
    #[cfg(not(windows))]
    {
        provider
            .inner
            .root
            .held(context)
            .and_then(|root| root.stat_file(file, context))
    }
}
