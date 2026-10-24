use super::*;

impl NativeSourceWatch {
    pub(crate) fn start_driver<W: SourceDemandReadWork>(
        root: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        Self::open_watch(root.directory_handle(), delivery, true, None, true, work)
    }
    /// Separate filters preserve the cause of a notification. In particular, an
    /// access stamp can never turn into a content read and feed itself back.
    pub(crate) fn open_capture_watch<W: SourceDemandReadWork>(
        root: std::os::windows::io::BorrowedHandle<'_>,
        delivery: Arc<Delivery>,
        complete: bool,
        recursive: bool,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        let carrier = delivery.prepare_startup_failure(work)?;
        let mut watch = Self::open_watch(
            root,
            Arc::clone(&delivery),
            false,
            Some(complete),
            recursive,
            work,
        )?;
        let bytes = size_of::<Self>() as u64;
        let mut backing_claimed = false;
        let result = (|| {
            work.claim_allocation(bytes, 1, WorkCounters::UNCHARGED)?;
            backing_claimed = true;
            watch.shared.ownership.retain(bytes, work)?;
            let mut access = Delivery::new(Arc::clone(&delivery.sink), work)?;
            access.read_authority = Some(Arc::clone(&delivery));
            access.access_only = true;
            let access = access.start(work, |access, work| {
                // One recursive metadata channel covers every admitted child;
                // child content registrations retain their original cost.
                Self::open_watch(root, access, false, Some(complete), true, work)
            })?;
            watch.access = Some(Box::new(access));
            Ok(())
        })();
        if backing_claimed && !work.is_native_original() {
            work.release(bytes)?;
        }
        match result {
            Ok(()) => Ok(watch),
            Err(error) => {
                let cleanup = watch.stop(work, true, None);
                if !watch.is_stopped() {
                    if let Some(carrier) = carrier {
                        return Err(carrier.retain_failed_platform(error, watch));
                    }
                }
                cleanup.and(Err(error))
            }
        }
    }
    #[allow(
        clippy::too_many_lines,
        reason = "This state transition keeps admission, effects and resource retirement in one frame."
    )]
    pub(crate) fn open_watch<W: SourceDemandReadWork>(
        root: std::os::windows::io::BorrowedHandle<'_>,
        delivery: Arc<Delivery>,
        pin_name: bool,
        complete: Option<bool>,
        recursive: bool,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        work.admit(READ)?;
        let carrier = delivery.prepare_startup_failure(work)?;
        let budget = work.with_original_leaf(|work| work.budget);
        let native = work.is_native_original();
        let ownership = SourcePayloadOwnership::new(work)?;
        let mut shared = None;
        let mut bytes = 0;
        let result = (|| {
            let mut active = work
                .native_source_work()
                .map(|original| original.allocation_work().reserve_work(io_work(3, 10)))
                .transpose()
                .map_err(crate::native_name::original_name_error)?;
            let terminal = active
                .as_mut()
                .map(|credit| credit.partition_work(io_work(1, 5), None))
                .transpose()
                .map_err(crate::native_name::original_name_error)?;
            if !native {
                reserve_cleanup(work, io_work(2, 5))?;
            }
            for allocation in [
                size_of::<OVERLAPPED>() as u64,
                BUFFER_BYTES,
                super::super::arc_control_bytes::<Shared>(),
            ] {
                work.claim_allocation(allocation, 1, WorkCounters::UNCHARGED)?;
                bytes += allocation;
            }
            ownership.retain(bytes, work)?;
            shared = Some(Arc::new(Shared {
                directory: AtomicPtr::new(std::ptr::null_mut()),
                completed: AtomicPtr::new(std::ptr::null_mut()),
                thread: Mutex::new(None),
                extended: AtomicBool::new(true),
                complete: AtomicBool::new(complete.unwrap_or(false)),
                recursive,
                reads: Mutex::new(Reads {
                    overlapped: Box::default(),
                    buffer: Some(Box::<[u64; BUFFER_WORDS]>::new_uninit()),
                    pending: false,
                    filled: 0,
                    cursor: 0,
                    loss: false,
                    blocked: false,
                }),
                buffer_owned: AtomicBool::new(true),
                reading_changed: Condvar::new(),
                stopping: AtomicBool::new(false),
                stop_completed: AtomicBool::new(false),
                ownership,
                cleanup: Mutex::new([active, terminal]),
                dropped: AtomicBool::new(false),
                #[cfg(test)]
                native_wait_cancellation: Mutex::new(None),
                delivery,
            }));
            let state = shared.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
            if native {
                *state.thread.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some(create_reader(state, work)?);
            }
            state.directory.store(
                open(root, pin_name, work)?.into_raw_handle(),
                Ordering::Release,
            );
            if complete.is_none() {
                // SAFETY: startup owns the installed handle until retirement begins.
                let directory = unsafe {
                    std::os::windows::io::BorrowedHandle::borrow_raw(
                        state.directory.load(Ordering::Acquire),
                    )
                };
                state.complete.store(
                    crate::native_host::reports_every_change(&directory, work)?,
                    Ordering::Release,
                );
            }
            work.started_io(WRITE)?;
            // SAFETY: one unnamed manual-reset event, owned by this state.
            let event =
                unsafe { CreateEventW(None, true, false, None) }.map_err(io::Error::from)?;
            state.completed.store(event.0, Ordering::Release);
            let mut reads = state.lock_reads();
            if let Err(error) = state.issue(&mut reads, work) {
                if !matches!(&error, DemandError::Io(error) if error.raw_os_error() == Some(ERROR_INVALID_FUNCTION.to_hresult().0))
                {
                    return Err(error);
                }
                state.extended.store(false, Ordering::Release);
                state.issue(&mut reads, work)?;
            }
            if !native {
                *state.thread.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some(create_reader(state, work)?);
            }
            drop(reads);
            state.reading_changed.notify_all();
            Ok(Self {
                shared: shared.take().ok_or(DemandError::NativeWorkUnavailable)?,
                access: None,
            })
        })();
        if !native {
            work.with_original_leaf(|work| work.budget = budget);
        }
        let transferred = shared.is_some() || result.is_ok();
        let result = match result {
            Ok(watch) => Ok(watch),
            Err(error) => match shared {
                None => Err(error),
                Some(shared) => {
                    let failed = Self {
                        shared,
                        access: None,
                    };
                    let cleanup = failed.stop(work, true, None);
                    if !failed.is_stopped() {
                        if !native {
                            work.release(bytes)?;
                        }
                        if let Some(carrier) = carrier {
                            return Err(carrier.retain_failed_platform(error, failed));
                        }
                    }
                    cleanup.and(Err(error))
                }
            },
        };
        if !native || !transferred {
            work.release(bytes)?;
        }
        result
    }
}

pub(super) fn reserve_cleanup<W: SourceDemandReadWork>(
    work: &mut W,
    delta: WorkCounters,
) -> Result<(), DemandError> {
    work.with_original_leaf(|work| super::super::reserve_cleanup_credits(work, delta))
}
fn create_reader<W: SourceDemandReadWork>(
    state: &Arc<Shared>,
    work: &mut W,
) -> Result<OwnedHandle, DemandError> {
    work.admit(io_work(1, 1))?;
    work.started_io(WRITE)?;
    let parameter = Arc::into_raw(Arc::clone(state));
    // SAFETY: successful creation transfers this Arc to the callback.
    match unsafe {
        CreateThread(
            None,
            0,
            Some(super::reader::reader_thread),
            Some(parameter.cast()),
            Default::default(),
            None,
        )
    } {
        Ok(handle) => {
            // SAFETY: successful CreateThread returned one owned handle.
            Ok(unsafe { OwnedHandle::from_raw_handle(handle.0) })
        }
        Err(error) => {
            // SAFETY: failed creation transferred no callback owner.
            drop(unsafe { Arc::from_raw(parameter) });
            Err(io::Error::from(error).into())
        }
    }
}
fn open<W: SourceDemandReadWork>(
    root: std::os::windows::io::BorrowedHandle<'_>,
    pin_name: bool,
    work: &mut W,
) -> Result<OwnedHandle, DemandError> {
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Wdk::Storage::FileSystem::{
        FILE_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_FOR_BACKUP_INTENT, NtCreateFile,
    };
    use windows::Win32::Foundation::{RtlNtStatusToDosError, UNICODE_STRING};
    use windows::Win32::Storage::FileSystem::{FILE_FLAGS_AND_ATTRIBUTES, FILE_SHARE_DELETE};
    use windows::Win32::System::IO::IO_STATUS_BLOCK;
    let name = UNICODE_STRING::default();
    let attributes = OBJECT_ATTRIBUTES {
        Length: u32::try_from(size_of::<OBJECT_ATTRIBUTES>())
            .map_err(|_| DemandError::InvalidRequest)?,
        RootDirectory: HANDLE(root.as_raw_handle()),
        ObjectName: &raw const name,
        ..OBJECT_ATTRIBUTES::default()
    };
    let mut handle = HANDLE::default();
    let mut status_block = IO_STATUS_BLOCK::default();
    work.started_io(READ)?;
    // SAFETY: all pointers remain live throughout this synchronous call.
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            FILE_LIST_DIRECTORY,
            &attributes,
            &mut status_block,
            None,
            FILE_FLAGS_AND_ATTRIBUTES(0),
            FILE_SHARE_READ
                | FILE_SHARE_WRITE
                | if pin_name {
                    Default::default()
                } else {
                    FILE_SHARE_DELETE
                },
            FILE_OPEN,
            FILE_DIRECTORY_FILE | FILE_OPEN_FOR_BACKUP_INTENT,
            None,
            0,
        )
    };
    if status.is_err() {
        // SAFETY: pure status translation.
        return Err(io::Error::from_raw_os_error(
            unsafe { RtlNtStatusToDosError(status) }.cast_signed(),
        )
        .into());
    }
    // SAFETY: successful creation transfers one owned handle.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle.0) })
}
