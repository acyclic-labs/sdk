use super::*;

pub(in crate::demand) struct RemoteReadPermit;

impl Drop for RemoteReadPermit {
        fn drop(&mut self) {
            BLOCKED_REMOTE_READS.fetch_sub(1, Ordering::AcqRel);
        }
    }

pub(in crate::demand) fn check_read_range(offset: u64, length: u64) -> Result<(), DemandError> {
        if length > 1024 * 1024 || offset.checked_add(length).is_none() {
            return Err(DemandError::InvalidRequest);
        }
        Ok(())
    }

pub(in crate::demand) struct NativeDemandFile {
        pub(in crate::demand) provider: NativeDemandSource,
        pub(in crate::demand) source: SourceReference,
        pub(in crate::demand) relative: DemandPath,
        pub(in crate::demand) expected: SourceVersion,
        pub(in crate::demand) logical_bytes: u64,
        #[cfg(windows)]
        pub(in crate::demand) file: Arc<NativeSourceFileSlot>,
        #[cfg(not(windows))]
        pub(in crate::demand) file: Arc<NativeSourceFile>,
        pub(in crate::demand) ownership: SourcePayloadOwnership,
    }

#[cfg(windows)]
    pub(in crate::demand) type SourceFile = crate::native_host::YieldingFile;

#[cfg(not(windows))]
    pub(in crate::demand) type SourceFile = std::fs::File;

pub(in crate::demand) struct NativeSourceFile {
        pub(in crate::demand) file: SourceFile,
        pub(in crate::demand) ownership: SourcePayloadOwnership,
    }

#[cfg(windows)]
    pub(in crate::demand) struct NativeSourceFileSlot {
        pub(in crate::demand) file: std::sync::RwLock<Arc<NativeSourceFile>>,
        pub(in crate::demand) ownership: SourcePayloadOwnership,
    }

pub(in crate::demand) const REMOTE_READ_DEADLINE: std::time::Duration = std::time::Duration::from_secs(20);

pub(in crate::demand) const MAXIMUM_BLOCKED_REMOTE_READS: usize = 16;

pub(in crate::demand) static BLOCKED_REMOTE_READS: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);

impl NativeDemandFile {
    pub(in crate::demand) fn open_boxed<W: SourceDemandReadWork>(
        provider: NativeDemandSource, source: SourceReference,
        path: &NamespacePath, expected: SourceVersion, work: &mut W,
    ) -> Result<Box<dyn DemandFile>, DemandError> {
        use crate::materializer::NativeResourceCredit::ReturnedItems;
        work.with_original_leaf(|work| work.reserve(ReturnedItems, 1))?;
        let bytes = std::mem::size_of::<Self>() as u64;
        if let Err(error) = work.allocate(bytes) {
            let _cancelled = work.with_original_leaf(|work| work.refund(ReturnedItems, 1));
            return Err(error);
        }
        let result = (|| {
            let file = Self::open(provider, source, path, expected, work)?;
            file.ownership.retain(bytes, work)?;
            Ok(Box::new(file) as Box<dyn DemandFile>)
        })();
        match result {
            Ok(file) => {
                work.with_original_leaf(|work| work.complete(ReturnedItems, 1, 1))?;
                Ok(file)
            }
            Err(error) => {
                let _released = work.release(bytes);
                let _cancelled = work.with_original_leaf(|work| work.refund(ReturnedItems, 1));
                Err(error)
            }
        }
    }

    pub(in crate::demand) fn open<W: SourceDemandReadWork>(
                provider: NativeDemandSource,
                source: SourceReference,
                path: &NamespacePath,
                expected: SourceVersion,
                work: &mut W,
            ) -> Result<Self, DemandError> {
                provider.check(source, work)?;
                provider.observe_parent(path, work)?;
                let ownership = SourcePayloadOwnership::new(work)?;
                let relative = provider.relative(path, work)?;
                ownership.retain(path_owned_bytes(&relative), work)?;
                let file_ownership = SourcePayloadOwnership::new(work)?;
                #[cfg(windows)]
                let file = provider.inner.root.open_file_yielding_wide_with_work(&relative, work);
                #[cfg(not(windows))]
                let file = provider
                    .inner
                    .root
                    .open_file(&relative, work)
                    .map(cap_std::fs::File::into_std);
                let file = match file {
                    Ok(file) => file,
                    Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                        return Err(DemandError::Absent);
                    }
                    Err(error) => return Err(error),
                };
                #[cfg(windows)]
                file_ownership.retain(file.logical_owned_bytes(), work)?;
                let opened = stat_held(&provider, &file, work)?;
                if !opened.file_type().is_file() {
                    return Err(DemandError::NotRegularFile);
                }
                if version(&opened, work)? != expected {
                    return Err(DemandError::StaleVersion);
                }
                let file_bytes = std::mem::size_of::<NativeSourceFile>() as u64;
                work.allocate(file_bytes)?;
                file_ownership.retain(file_bytes, work)?;
                let file = Arc::new(NativeSourceFile { file, ownership: file_ownership });
                #[cfg(windows)]
                let file = {
                    let slot_ownership = SourcePayloadOwnership::new(work)?;
                    let bytes = std::mem::size_of::<NativeSourceFileSlot>() as u64;
                    work.allocate(bytes)?;
                    slot_ownership.retain(bytes, work)?;
                    Arc::new(NativeSourceFileSlot {
                        file: std::sync::RwLock::new(file), ownership: slot_ownership,
                    })
                };
                Ok(Self {
                    provider,
                    source,
                    relative,
                    expected,
                    logical_bytes: opened.len(),
                    file,
                    ownership,
                })
            }

    pub(in crate::demand) fn read<W: SourceDemandReadWork>(
                &self,
                offset: u64, length: u64,
                work: &mut W,
            ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
                #[cfg(windows)]
                let file = self
                    .file
                    .file
                    .read()
                    .map_err(|_| DemandError::OriginalWorkPoisoned)?
                    .clone();
                #[cfg(not(windows))]
                let file = Arc::clone(&self.file);
                let result = self.read_once(&file.file, offset, length, work);
                #[cfg(windows)]
                if matches!(result, Err(DemandError::StaleVersion)) && file.file.was_yielded() {
                    let reopened = Arc::new(self.reopen(work)?);
                    let current = {
                        let mut held = self
                            .file
                            .file
                            .write()
                            .map_err(|_| DemandError::OriginalWorkPoisoned)?;
                        if Arc::ptr_eq(&*held, &file) {
                            *held = reopened;
                        }
                        Arc::clone(&*held)
                    };
                    return self.read_once(&current.file, offset, length, work);
                }
                result
            }

    #[cfg(windows)]
            pub(in crate::demand) fn reopen<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<NativeSourceFile, DemandError> {
                self.provider.check(self.source, work)?;
                let ownership = SourcePayloadOwnership::new(work)?;
                let file = match self.provider.inner.root.open_file_yielding_wide_with_work(&self.relative, work) {
                    Ok(file) => file,
                    Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                        return Err(DemandError::StaleVersion);
                    }
                    Err(error) => return Err(error),
                };
                ownership.retain(file.logical_owned_bytes(), work)?;
                let opened = stat_held(&self.provider, &file, work)?;
                if version(&opened, work)? != self.expected {
                    return Err(DemandError::StaleVersion);
                }
                let bytes = std::mem::size_of::<NativeSourceFile>() as u64;
                work.allocate(bytes)?;
                ownership.retain(bytes, work)?;
                Ok(NativeSourceFile { file, ownership })
            }

    pub(in crate::demand) fn read_once<W: SourceDemandReadWork>(
                &self,
                file: &SourceFile,
                offset: u64, length: u64,
                work: &mut W,
            ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
                work.check()?;
                let length = length.min(self.logical_bytes.saturating_sub(offset));
                let size = usize::try_from(length).map_err(|_| DemandError::InvalidRequest)?;
                work.admit(WorkCounters { output_bytes: length, ..WorkCounters::UNCHARGED })?;
                if work.is_native_original() {
                    work.admit(WorkCounters { source_bytes_read: length, ..WorkCounters::UNCHARGED })?;
                }
                let mut ownership = Some(SourcePayloadOwnership::new(work)?);
                let (mut bytes, capacity) = source_vec::<u8, _>(size, work)?;
                if let Err(error) = ownership.as_ref().expect("untransferred source custody").retain(capacity, work) {
                    drop(bytes);
                    let _cleanup = work.release(capacity);
                    return Err(error);
                }
                let mut prepared = if work.is_native_original() {
                    let original = work.native_source_work().ok_or(DemandError::NativeWorkUnavailable)?
                        .materialization_work()?;
                    let copied = original.reserve_work(crate::WorkCounters { bytes_copied: capacity, ..crate::WorkCounters::UNCHARGED }).map_err(crate::native_name::original_name_error)?;
                    bytes.resize(bytes.capacity(), 0);
                    { let mut copy = copied; copy.record_completed(crate::WorkCounters { bytes_copied: capacity, ..crate::WorkCounters::UNCHARGED }) }.map_err(crate::native_name::original_name_error)?;
                    Some(acyclic_stream::PreparedOwnedBytes::new(
                        std::mem::take(&mut bytes), ownership.take().expect("original source custody"),
                        original.runtime_meter(),
                    ).map_err(|failure| match failure.into_error() {
                        acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                            crate::native_name::original_name_error(original.runtime_error(error))
                        }
                        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
                        => DemandError::InvalidRequest,
                    })?)
                } else {
                    bytes.resize(size, 0);
                    None
                };
                let result = (|| {
                    let mut filled = 0;
                    let destination = match prepared.as_mut() {
                        Some(prepared) => &mut prepared.writable()[..size],
                        None => bytes.as_mut_slice(),
                    };
                    while let Some(unfilled) = destination.get_mut(filled..).filter(|rest| !rest.is_empty()) {
                        work.check()?;
                        #[cfg(unix)]
                        let read = work.with_native_read_meter(|meter| {
                            acyclic_native_runtime::read_declared_source_bytes(
                                file, offset + filled as u64, unfilled, meter,
                            )
                        }).map_err(native_demand_error);
                        #[cfg(windows)]
                        let read = file.read_at(offset + filled as u64, unfilled, work);
                        match read {
                            Ok(0) => break,
                            Ok(count) => filled += count,
                            Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted => {}
                            Err(error) => return Err(error),
                        }
                    }
                    self.prove_current(file, work)?;
                    work.charge(WorkCounters { output_bytes: filled as u64, ..WorkCounters::UNCHARGED })?;
                    if !work.is_native_original() && filled != bytes.capacity() {
                        work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::UNCHARGED })?;
                    }
                    Ok(filled)
                })();
                match result {
                    Ok(filled) => {
                        if let Some(prepared) = prepared {
                            let bytes = prepared.freeze(filled).map_err(|failed| {
                                drop(failed);
                                DemandError::InvalidRequest
                            })?;
                            Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, capacity))
                        } else {
                            bytes.truncate(filled);
                            Ok(crate::kernel::OwnedSourceChunk::claimed(bytes.into(), capacity))
                        }
                    }
                    Err(error) => {
                        drop(prepared);
                        drop(bytes);
                        drop(ownership);
                        if !work.is_native_original() { work.release(capacity)?; }
                        Err(error)
                    }
                }
            }

    pub(in crate::demand) fn prove_current<W: SourceDemandReadWork>(
                &self,
                file: &SourceFile,
                work: &mut W,
            ) -> Result<(), DemandError> {
                let held = stat_held(&self.provider, file, work)?;
                let named = self.provider.stat_relative(&self.relative, work);
                if matches!(named, Err(DemandError::Work(_) | DemandError::Cancelled)) {
                    return named.map(|_| ());
                }
                let held_version = version(&held, work)?;
                let named_version = if held_version == self.expected {
                    named.as_ref().ok().map(|metadata| version(metadata, work)).transpose()?
                } else { None };
                if held_version != self.expected || named_version != Some(self.expected)
                {
                    // A replaced root renames every file under it: report that.
                    self.provider.check(self.source, work)?;
                    return Err(DemandError::StaleVersion);
                }
                self.provider.check(self.source, work)
            }
}

impl DemandFile for NativeDemandFile {
        fn logical_owned_bytes(&self) -> u64 {
            let bytes = (std::mem::size_of::<Self>() + std::mem::size_of::<NativeSourceFile>()) as u64
                + path_owned_bytes(&self.relative);
            #[cfg(windows)]
            let bytes = bytes
                + std::mem::size_of::<NativeSourceFileSlot>() as u64
                + self.file.file.read().unwrap_or_else(std::sync::PoisonError::into_inner).file.logical_owned_bytes();
            bytes
        }

        fn read_range(
            &self, offset: u64, length: u64, accounting: &mut crate::kernel::KernelAccounting<'_>,
        ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
            if let crate::kernel::KernelAccounting::NativeOriginal { work: original, cancellation } = accounting {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                let mut work = NativeSourceWork::from_native_materialization_work(original);
                work.check()?;
                check_read_range(offset, length)?;
                let result = self.read(offset, length, &mut work);
                let result = work.with_original_leaf(DemandWork::take_native_io_failure).map_or(result, Err);
                let checked = cancellation.check().map_err(|_| DemandError::Cancelled);
                return result.and_then(|chunk| checked.map(|()| chunk));
            }
            check_read_range(offset, length)?;
            let budget = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
            let answer = {
                let (_, _, _, cancellation) = accounting.standalone_parts().ok_or(DemandError::NativeWorkUnavailable)?;
                let mut work = DemandWork::new(budget, cancellation);
                if let Err(error) = work.check() { work.finish(Err(error)) }
                else if self.provider.inner.inline {
                    let result = self.read(offset, length, &mut work);
                    let result = work.take_native_io_failure().map_or(result, Err);
                    work.finish(result)
                } else { self.read_remote(offset, length, work, REMOTE_READ_DEADLINE) }
            };
            accept_source_result(answer, accounting)
        }
    }

impl NativeDemandFile {
    pub(in crate::demand) fn read_remote(
                &self,
                offset: u64, length: u64,
                mut work: DemandWork,
                deadline: std::time::Duration,
            ) -> DemandResult<crate::kernel::OwnedSourceChunk> {
                if BLOCKED_REMOTE_READS.fetch_add(1, Ordering::AcqRel) >= MAXIMUM_BLOCKED_REMOTE_READS {
                    BLOCKED_REMOTE_READS.fetch_sub(1, Ordering::AcqRel);
                    return work.finish(Err(DemandError::SourceUnavailable));
                }
                let _permit = RemoteReadPermit;
                #[cfg(unix)]
                let relative = {
                    use std::os::unix::ffi::{OsStrExt, OsStringExt};
                    let bytes = self.relative.as_os_str().as_bytes();
                    let length = bytes.len() as u64;
                    if let Err(error) = work.allocate_for(length, WorkCounters {
                        bytes_copied: length, ..WorkCounters::UNCHARGED
                    }) { return work.finish(Err(error)); }
                    PathBuf::from(std::ffi::OsString::from_vec(bytes.to_vec()))
                };
                #[cfg(windows)]
                let relative = {
                    let length = self.relative.len() as u64 * 2;
                    if let Err(error) = work.allocate_for(length, WorkCounters {
                        bytes_copied: length, ..WorkCounters::UNCHARGED
                    }) { return work.finish(Err(error)); }
                    self.relative.to_vec()
                };
                let file = Self {
                    provider: self.provider.clone(), source: self.source, relative,
                    expected: self.expected, logical_bytes: self.logical_bytes,
                    file: Arc::clone(&self.file),
                    ownership: SourcePayloadOwnership::default(),
                };
                if let Err(error) = work.allocate(std::mem::size_of::<std::sync::atomic::AtomicBool>() as u64) {
                    return work.finish(Err(error));
                }
                let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
                work.stop = Some(Arc::clone(&stop));
                let adapter_bytes = std::mem::size_of::<NativeDemandReadMeter>() as u64;
                if let Err(error) = work.allocate(adapter_bytes) {
                    return work.finish(Err(error));
                }
                let budget = work.budget;
                let caller_cancellation = work.cancellation.clone();
                let shared = Arc::new(NativeDemandReadMeter(Mutex::new(work)));
                let mut prepared = {
                    let mut work = shared.lock();
                    match work.prepare_cancelled(&caller_cancellation) {
                        Ok(prepared) => {
                            work.budget.items_examined -= prepared.cleanup_items();
                            prepared
                        }
                        Err(error) => {
                            drop(work);
                            return crate::NativeMaterializationWork::from_meter(shared)
                                .finish_source(Err(cancellation_error(error)));
                        }
                    }
                };
                let worker_work = Arc::clone(&shared);
                let task_meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = shared.clone();
                let task = acyclic_native_runtime::run_blocking_io(
                    crate::obs::in_context(crate::obs::caller_context(), move || {
                        let mut work = NativeSourceWork::ordinary(&worker_work);
                        let path_bytes = path_owned_bytes(&file.relative);
                        let result = file.read(offset, length, &mut work);
                        drop(file);
                        let released = work.release(path_bytes);
                        released.and(result)
                    }),
                    Some((task_meter, false)),
                );
                let mut expired = false;
                let result = match task {
                    Ok(task) => task.wait(std::time::Instant::now().checked_add(deadline), &mut prepared, || {
                        expired = true;
                        stop.store(true, Ordering::Release);
                    }).map_err(native_demand_error).and_then(std::convert::identity),
                    Err(error) => Err(native_demand_error(error)),
                };
                prepared.retire_waker();
                let original = crate::NativeMaterializationWork::from_meter(shared);
                let closed = original.source_leaf(|work| {
                    work.restore_budget(budget);
                    let cleanup = work.finish_cancelled(prepared).map_err(cancellation_error);
                    (cleanup, work.take_native_io_failure(), work.check(), work.stop.take())
                });
                let result = match closed {
                    Ok((cleanup, failure, checked, owned_stop)) => {
                        drop(owned_stop);
                        drop(stop);
                        let released = original.source_leaf(|work| work.release(
                            std::mem::size_of::<std::sync::atomic::AtomicBool>() as u64,
                        )).and_then(std::convert::identity);
                        let result = failure.map_or(result, Err);
                        let result = if expired && !caller_cancellation.is_cancelled()
                            && matches!(&result, Ok(_) | Err(DemandError::Cancelled)) {
                            Err(DemandError::SourceUnavailable)
                        } else { result.and_then(|value| checked.map(|()| value)) };
                        released.and(cleanup).and(result)
                    }
                    Err(error) => Err(error),
                };
                original.finish_source(result)
            }
}

pub(in crate::demand) fn stat_held<W: SourceDemandReadWork>(
        provider: &NativeDemandSource, file: &SourceFile, work: &mut W,
    ) -> Result<HostStat, DemandError> {
        #[cfg(windows)]
        { file.with(|file| provider.inner.root.stat_file_with_work(file, work)) }
        #[cfg(not(windows))]
        { provider.inner.root.stat_file_with_work(file, work) }
    }

pub(in crate::demand) fn node_kind(file_type: crate::native_host::HostFileType) -> SourceNodeKind {
        if file_type.is_dir() {
            SourceNodeKind::Directory
        } else if file_type.is_file() {
            SourceNodeKind::RegularFile
        } else if file_type.is_symlink() {
            SourceNodeKind::SymbolicLink
        } else {
            special_node_kind(&file_type)
        }
    }

#[cfg(unix)]
    pub(in crate::demand) fn special_node_kind(file_type: &crate::native_host::HostFileType) -> SourceNodeKind {
        if file_type.is_fifo() {
            SourceNodeKind::Fifo
        } else if file_type.is_socket() {
            SourceNodeKind::Socket
        } else if file_type.is_char_device() {
            SourceNodeKind::CharacterDevice
        } else if file_type.is_block_device() {
            SourceNodeKind::BlockDevice
        } else {
            SourceNodeKind::Unsupported
        }
    }

#[cfg(not(unix))]
    pub(in crate::demand) fn special_node_kind(_file_type: &crate::native_host::HostFileType) -> SourceNodeKind {
        SourceNodeKind::Unsupported
    }
