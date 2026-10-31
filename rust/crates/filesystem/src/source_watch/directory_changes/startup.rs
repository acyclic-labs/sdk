use super::*;

impl NativeSourceWatch {
    pub(crate) fn start_driver<W: SourceDemandReadWork>(
        root: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        Self::open_watch(root.directory_handle(), delivery, true, None, true, work)
    }
    pub(crate) fn open_watch<W: SourceDemandReadWork>(
        root: std::os::windows::io::BorrowedHandle<'_>,
        delivery: Arc<Delivery>,
        pin_name: bool,
        complete: Option<bool>,
        recursive: bool,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        Self::open_watch_growing_to(
            root,
            delivery,
            pin_name,
            complete,
            recursive,
            MAXIMUM_BUFFER_BYTES,
            work,
        )
    }
    #[allow(
        clippy::too_many_lines,
        reason = "This state transition keeps admission, effects and resource retirement in one frame."
    )]
    pub(super) fn open_watch_growing_to<W: SourceDemandReadWork>(
        root: std::os::windows::io::BorrowedHandle<'_>,
        delivery: Arc<Delivery>,
        pin_name: bool,
        complete: Option<bool>,
        recursive: bool,
        maximum_buffer_bytes: u64,
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
                .map(|original| original.allocation_work().reserve_work(io_work(2, 6)))
                .transpose()
                .map_err(crate::native_name::original_name_error)?;
            let terminal = active
                .as_mut()
                .map(|credit| credit.partition_work(io_work(1, 3), None))
                .transpose()
                .map_err(crate::native_name::original_name_error)?;
            if !native {
                reserve_cleanup(work, io_work(1, 2))?;
            }
            hub::ensure()?;
            for allocation in [
                size_of::<OVERLAPPED>() as u64,
                INITIAL_BUFFER_BYTES,
                super::super::arc_control_bytes::<Shared>(),
            ] {
                work.claim_allocation(allocation, 1, WorkCounters::UNCHARGED)?;
                bytes += allocation;
            }
            ownership.retain(bytes, work)?;
            shared = Some(Arc::new(Shared {
                directory: AtomicPtr::new(std::ptr::null_mut()),
                extended: AtomicBool::new(true),
                complete: AtomicBool::new(complete.unwrap_or(false)),
                recursive,
                pin_name,
                maximum_buffer_bytes,
                reads: Mutex::new(Reads {
                    overlapped: Box::default(),
                    buffer: Some(buffer(INITIAL_BUFFER_BYTES)),
                    pending: false,
                    filled: 0,
                    cursor: 0,
                    loss: false,
                    blocked: false,
                    cancelling: false,
                }),
                buffer_bytes: AtomicU64::new(INITIAL_BUFFER_BYTES),
                arrived: Condvar::new(),
                arrivals: Mutex::new(()),
                missed: AtomicBool::new(false),
                stopping: AtomicBool::new(false),
                stop_completed: AtomicBool::new(false),
                parked: AtomicBool::new(false),
                ownership,
                cleanup: Mutex::new([active, terminal]),
                dropped: AtomicBool::new(false),
                #[cfg(test)]
                native_wait_cancellation: Mutex::new(None),
                delivery,
            }));
            let state = shared.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
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
            hub::attach(state, HANDLE(state.directory.load(Ordering::Acquire)))?;
            let mut reads = state.lock_reads();
            if let Err(error) = state.issue(&mut reads, work) {
                if !matches!(&error, DemandError::Io(error) if error.raw_os_error() == Some(ERROR_INVALID_FUNCTION.to_hresult().0))
                {
                    return Err(error);
                }
                state.extended.store(false, Ordering::Release);
                state.issue(&mut reads, work)?;
            }
            drop(reads);
            Ok(Self {
                shared: shared.take().ok_or(DemandError::NativeWorkUnavailable)?,
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
                    let failed = Self { shared };
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
pub(super) fn open<W: SourceDemandReadWork>(
    root: std::os::windows::io::BorrowedHandle<'_>,
    pin_name: bool,
    work: &mut W,
) -> Result<OwnedHandle, DemandError> {
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Wdk::Storage::FileSystem::{
        FILE_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_FOR_BACKUP_INTENT, NtCreateFile,
    };
    use windows::Win32::Foundation::{RtlNtStatusToDosError, UNICODE_STRING};
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAGS_AND_ATTRIBUTES, FILE_SHARE_DELETE, SYNCHRONIZE,
    };
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
            // Waiting for a read without an event waits on the handle.
            FILE_LIST_DIRECTORY | SYNCHRONIZE,
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
